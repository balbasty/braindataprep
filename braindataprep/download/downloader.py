
import requests
import time
import random
import os
import datetime
from enum import Enum as _Enum
from logging import getLogger
from os.path import lexists
from pathlib import Path, PosixPath
from typing import Literal, Generator, Iterator, Callable
from urllib.parse import urlparse, ParseResult

from braindataprep.digests import get_digest
from braindataprep.digests import sort_digests
from braindataprep.download.remote import RemoteFile
from braindataprep.download.incomplete import IncompleteFile
from braindataprep.download.constants import CHUNK_SIZE

lg = getLogger('__name__')


class IfExists:
    """
    This class both:
    - holds the set of singleton values (as an Enum)
    - defines constant (such as the default value)
    - serves as a context manager to override whichever value was set

    ```python
    action = Action(..., ifexists='overwrite')
    with IfExists('refresh'):
        yield from action
    ```
    """

    Choice = Literal['skip', 'overwrite', 'different', 'refresh', 'error']

    class Enum(_Enum):
        SKIP = S = 1
        OVERWRITE = O = 2       # noqa: E741
        DIFFERENT = D = 3
        REFRESH = R = 4
        ERROR = E = 5

    # Expose values
    SKIP = Enum.SKIP
    OVERWRITE = Enum.OVERWRITE
    DIFFERENT = Enum.DIFFERENT
    REFRESH = Enum.REFRESH
    ERROR = Enum.ERROR

    # Set (class attribute) default
    default: Enum = DIFFERENT
    current: Enum | None = None

    @classmethod
    def from_any(cls, x: str | int | Enum | None) -> Enum:
        """Return the singleton representation of a value"""
        if x is None:
            return cls.default
        elif isinstance(x, str):
            return getattr(cls.Enum, x[0].upper())
        else:
            return cls.Enum(x)

    def __init__(self, value: Choice | Enum) -> None:
        self.value = self.from_any(value)
        self._prev = None

    def __enter__(self) -> None:
        self._prev = self.default
        self.current = self.value

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.current = self._prev
        self._prev = None


class Downloader:
    """
    An object that knows how to download a file.

    While downloading, the downloader yields regular status messages.
    Possible status are

    * intermediate status:
        {
            'done': int,        # total number of bytes downloaded
            'done%': float,     # percentage of the file downloaded
        }
    * error status:
        {
            'status': 'error',  # An error happended
            'message': str,     # Description of the error
        }
    * file finished downloading but checksum differs:
        {
            'checksum': 'differs',  # Checksum if not the same as expected
            'status': 'error',      # An error happended
            'message': msg,         # Description of the error
        }
    * file finished downloading and checksum matches:
        {
            'checksum': 'ok',
            'status': 'done',
        }
    * file finished downloading and checksum cannot be checked:
        {
            'checksum': '-',        # Checksum was not provided
            'status': 'done',
        }

    ```python
    downloader = Downloader(url, filename)
    for status in downloader:
        if 'done' in status:
            print('downloaded: {status["done"]}B', end='')
            if 'done%' in status:
                print('({status["done%"]}%)', end='')
            print(end='\r')
        else:
            print('\n')
            if status['status'] == 'error':
                print('error: {status["message"]}')
            if 'checksum' in status:
                print('checksum: {status["checksum"]}')
            if status['status'] == 'done':
                print('done.')
    ```
    """

    # Derived from `dandi.download`
    # https://github.com/dandi/dandi-cli/blob/master/dandi/download.py
    # Apache License Version 2.0

    RETRY_STATUSES: list[int] = [
        400,    # Bad Request - https://github.com/dandi/dandi-cli/issues/87
        500,    # Internal Server Error
        502,    # Bad Gateway
        503,    # Service Unavailable
        504,    # Gateway Timeout
    ]

    def __init__(
        self,
        src: str | ParseResult,
        dst: str | Path | None = None,
        *,
        ifexists: IfExists.Enum | IfExists.Choice = 'different',
        chunk_size: int = CHUNK_SIZE,
        session: requests.Session | None = None,
        auth: Callable[[requests.Session], None] = None,
        size: int | None = None,
        mtime: datetime.datetime | None = None,
        digests: dict[str, str] | None = None,
        ifnodigest: Literal['restart', 'continue'] = 'restart',
        max_attemps: int = 3,
    ):
        """
        Parameters
        ----------
        src : str | ParseResult
            Remote URL
        dst : str | Path | None
            Output filename

        Other Parameters
        ----------------
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh'}
            Behaviour if destination file already exists
        chunk_size : int
            Number of bytes to read at once
        session : Session
            Opened session
        size : int | None
            Expected size of the file
        mtime : datetime
            Expected last-modified time
        digests : dict | None
            Expected digest(s) of the file.
            Keys are algorithm names (e.g. "sha256") and values are the
            digests.
        ifnodigest : {'restart', 'continue'}
            Behaviour if incomplete file exists but no digest is provided
        max_attempts : int
            Maximum number of attempts
        """
        if not isinstance(src, ParseResult):
            src = urlparse(src)
        self.src = src
        dst = Path(dst or '.')
        if dst.is_dir():
            dst = dst / PosixPath(src.path).name
        self.dst = dst
        self.session = session
        self.auth = auth
        self.size = size
        self.mtime = mtime
        self.chunk_size = chunk_size
        if digests:
            digests = sort_digests(digests)
        self.digests = digests
        self.ifnodigest = ifnodigest
        self.ifexists = IfExists.from_any(ifexists)
        self.max_attemps = max_attemps

    def _should_overwrite(self) -> Generator[dict, None, bool]:
        if not lexists(self.dst):
            lg.info(f'File {self.dst!s} does not exits: download')
            return True

        ifexists = IfExists.current or self.ifexists

        if ifexists is IfExists.ERROR:
            lg.error(f'File {self.dst!s} already exists')
            raise FileExistsError(f'File {self.dst!s} already exists')

        elif ifexists is IfExists.SKIP:
            lg.info(f'File {self.dst!s} already exists: skip')
            yield {'status': 'skipped', 'message': 'already exists'}
            return False

        elif ifexists is IfExists.OVERWRITE:
            lg.info(f'File {self.dst!s} already exists: overwrite')
            return True

        elif ifexists is IfExists.DIFFERENT:
            if (
                self.size is not None and
                self.size != os.stat(self.dst.resolve()).st_size
            ):
                lg.info(
                    f'Size of {self.dst!s} does not match size on server; '
                    'redownloading'
                )
                return True

            if self.digests:
                checkalgo, checksum = next(iter(self.digests.items()))
                local_checksum = get_digest(self.dst, checkalgo)
                if checksum == local_checksum:
                    lg.info(f'File {self.dst!s} is same as remote: skip')
                    yield {'status': 'skipped', 'message': 'already exists'}
                    return False
                else:
                    lg.info(
                        f'Checksum of {self.dst!s} does not match '
                        f'checksum on server; redownloading', self.dst
                    )

            return True

        elif ifexists is IfExists.REFRESH:
            if self.mtime is None:
                lg.warning(
                    f'{self.dst!s} - no mtime in the record, '
                    f'redownloading'
                )
                return True
            if self.size is None:
                lg.warning(
                    f'{self.dst!s} - no size in the record, '
                    f'redownloading'
                )
                return True
            local_stat = os.stat(self.dst.resolve())
            local_size = local_stat.st_size
            local_mtime = datetime.datetime.fromtimestamp(local_stat.st_mtime)
            local_mtime = local_mtime.astimezone(datetime.timezone.utc)
            if local_mtime == self.mtime and local_size == self.size:
                lg.info(f'File {self.dst!s} is fresh enough: skip')
                yield {'status': 'skipped', 'message': 'already exists'}
                return False

        lg.info(f'File {self.dst!s} is not fresh: redownload')
        return True

    def __iter__(self) -> Iterator[dict]:
        """
        Download the file
        """
        # --------------------------------------------------------------
        # Read size and mtime from remote
        # --------------------------------------------------------------
        if self.size is None or self.mtime is None:
            remote = RemoteFile(self.src, session=self.session, auth=self.auth)
            if self.size is None:
                self.size = remote.size
            if self.mtime is None:
                self.mtime = remote.mtime
        yield {'size': self.size}

        # --------------------------------------------------------------
        # If file exists, select replacement strategy
        # --------------------------------------------------------------
        if not (yield from self._should_overwrite()):
            return

        # --------------------------------------------------------------
        # Download
        # --------------------------------------------------------------
        for attempt in range(self.max_attemps):
            try:
                warned = False
                if self.digests:
                    checksum, checkalgo = next(iter(self.digests.items()))
                else:
                    checksum = checkalgo = None
                with IncompleteFile(
                    self.dst,
                    checksum=checksum,
                    checkalgo=checkalgo,
                    ifnochecksum=self.ifnodigest,
                ) as local_file:

                    assert local_file.offset is not None
                    downloaded = local_file.offset
                    if self.size is not None and downloaded == self.size:
                        # Exit early when downloaded == size, as making
                        # a Range request in such a case results in a
                        # 416 error from S3. Problems will result if
                        # `size` is None but we've already downloaded
                        # everything.
                        break

                    with RemoteFile(
                        self.src,
                        session=self.session,
                        auth=self.auth,
                        chunk_size=self.chunk_size,
                        offset=local_file.offset,
                    ) as remote_file:

                        for chunk in remote_file:
                            downloaded += len(chunk)
                            out = {'done': downloaded}
                            if self.size:
                                if downloaded > self.size and not warned:
                                    warned = True
                                    # Yield ERROR?
                                    lg.warning(
                                        'Downloaded %d bytes although size '
                                        'was told to be just %d.',
                                        downloaded, self.size,
                                    )
                                out['done%'] = 100 * downloaded / self.size
                            local_file += chunk
                            out['dspeed'] = remote_file.mean_speed
                            out['wspeed'] = local_file.mean_speed
                            yield out

                    dlchecksum = local_file.digest

                # ------------------------------------------------------
                # success! -> a few checks then break out of trials loop
                # ------------------------------------------------------

                if checksum and dlchecksum:

                    if dlchecksum != checksum:
                        msg = (
                            f'{checkalgo}: '
                            f'downloaded {dlchecksum} != {checksum}'
                        )
                        yield {
                            'checksum': 'differs',
                            'status': 'error',
                            'message': msg,
                        }
                        lg.debug(
                            '%s is different: %s.', self.dst, msg
                        )
                        return
                    else:
                        yield {'checksum': 'ok'}
                        lg.debug(
                            'Verified that %s has correct %s %s',
                            self.dst, checkalgo, dlchecksum
                        )

                else:
                    yield {'checksum': '-'}

                if self.mtime is not None:
                    yield {'status': 'setting mtime'}
                    os.utime(self.dst, (time.time(), self.mtime.timestamp()))

                yield {'status': 'done'}
                return

            # ----------------------------------------------------------
            # An exception was raised
            # ----------------------------------------------------------

            # When `requests` raises a ValueError, it's because the caller
            # provided invalid parameters (e.g., an invalid URL), and so
            # retrying won't change anything.
            except ValueError:
                raise

            # Catching RequestException lets us retry on timeout & connection
            # errors (among others) in addition to HTTP status errors.
            except requests.RequestException as exc:
                # TODO: actually we should probably retry only on
                # selected codes, and also respect Retry-After
                if 1 + attempt >= self.max_attemps or (
                    exc.response is not None
                    and exc.response.status_code not in self.RETRY_STATUSES
                ):
                    lg.debug('Download failed: %s', exc)
                    yield {'status': 'error', 'message': str(exc)}
                    return
                # if is_access_denied(exc) or attempt >= 2:
                #     raise
                # sleep a little and retry
                lg.debug(
                    'Failed to download on attempt #%d: %s, '
                    'will sleep a bit and retry',
                    attempt, exc,
                )
                time.sleep(random.random() * 5)
