import hashlib
import time
from pathlib import Path
from shutil import rmtree
from fasteners import InterProcessLock
from typing import IO, Literal
from logging import getLogger

from braindataprep.digests import get_digester

lg = getLogger(__name__)


class IncompleteFile:
    """
    An object that represents a file being downloaded, in its
    incomplete state. It is used as a context manager and checks whether
    an unfinished download can be continued, or should be completely
    restarted.

    ```python
    with IncompleteFile(filename, checksum=sha1) as obj:
        for chunk in chunk_server:
            obj += chunk
    ```
    """

    # Derived from `dandi.download.DownloadDirectory`
    # https://github.com/dandi/dandi-cli/blob/master/dandi/download.py
    # Apache License Version 2.0

    def __init__(
            self,
            filename: str | Path,
            checksum: str | None = None,
            checkalgo: str | None = None,
            ifnochecksum: Literal['restart', 'continue'] = 'restart',
    ):
        """
        Parameters
        ----------
        filename : str | Path
            Output filename
        checksum : str | None
            Expected checksum (hex) of the file
        checkalgo : str | None
            Algorithm to use to compute the checksum of downloaded file
        ifnochecksum : {'restart', 'continue'}
            Behaviour if incomplete file exists but no checksum is provided
        """
        # checks
        if checkalgo and not hasattr(checkalgo, hashlib):
            raise ValueError('Unknown hashing algorithm')
        # assign
        self.filename: Path = Path(filename)
        self.tempname: Path = self.filename.with_name(
            self.filename.name + '.download'
        )
        self.lockname: Path = self.filename.with_name(
            self.filename.name + '.lock'
        )
        self.checkname: Path = self.filename.with_name(
            self.filename.name + '.checksum'
        )
        self.lock: InterProcessLock | None = None
        self.file: IO[bytes] | None = None
        self.offset: int | None = None
        self.checksum: str = checksum
        self.checkalgo: str = checkalgo
        self.ifnochecksum: Literal['r', 'c'] = ifnochecksum.lower()[0]
        self.digester = None
        self._digest: str | None = None
        self.last_speed: float = 0
        self.mean_speed: float = 0

    @property
    def digest(self) -> str:
        if self._digest is not None:
            return self._digest
        elif self.digester:
            return self.digester.hexdigest()
        else:
            return None

    def __enter__(self) -> "IncompleteFile":
        self.filename.parent.mkdir(parents=True, exist_ok=True)

        # Acquire lock
        self.lock = InterProcessLock(str(self.lockname))
        if not self.lock.acquire(blocking=False):
            raise RuntimeError(
                f'Could not acquire download lock for {self.filename}'
            )

        # Check if a file was already being downloaded, and if we should
        # continue from where we left off
        try:
            with self.checkname.open('rt') as f:
                checksum = f.read()
        except (FileNotFoundError, ValueError):
            checksum = None

        # Compute checksum on the fly
        self._digest = None
        if self.checkalgo:
            self.digester = hashlib.new(self.checkalgo)

        # Check whether we should keep the existing partial file
        cont = self.tempname.exists()
        cont = cont and ((self.checksum and self.checksum == checksum) or
                         (not self.checksum and self.ifnochecksum == 'c'))
        if cont:
            mode = 'ab'
            if self.checksum:
                lg.debug(
                    'Download file exists and has matching checksum; '
                    'resuming download'
                )
            else:
                lg.debug(
                    'Download file exists; resuming download'
                )
            if self.digest:
                with self.tempname.open('rb') as f:
                    self.digester = get_digester(f, self.checkalgo)
        else:
            mode = 'wb'
            if self.tempname.exists():
                if self.checksum:
                    lg.debug(
                        'Download file found, but checksum does not match; '
                        'starting new download'
                    )
                else:
                    lg.debug(
                        'Download file exists; starting new download'
                    )
            else:
                lg.debug('Starting new download')
            # Remove existing file
            self.tempname.unlink(missing_ok=True)

        # Open file
        self.file = self.tempname.open(mode)
        self.offset = self.file.tell()

        # Write expected checksum
        if self.checksum:
            with self.checkname.open("w") as f:
                f.write(self.checksum)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Close file
        assert self.file is not None
        self.file.close()

        # Rename temporary filename to output filename
        # Note that we only rename the file to its final name and
        # remove temporary file if the download was succesful (i.e.
        # the context was not interrupted by an exception)
        try:
            if exc_type is None:
                try:
                    self.tempname.replace(self.filename)
                except IsADirectoryError:
                    rmtree(self.filename)
                    self.tempname.replace(self.filename)
                if self.digester:
                    self._digest = self.digester.hexdigest()
        finally:
            # Release lock and delete existing files
            assert self.lock is not None
            self.lock.release()
            if exc_type is None:
                self.tempname.unlink(missing_ok=True)
                self.lockname.unlink(missing_ok=True)
                self.checkname.unlink(missing_ok=True)
            self.lock = None
            self.file = None
            self.offset = None

    def append(self, blob: bytes) -> "IncompleteFile":
        if self.file is None:
            raise ValueError(
                'IncompleteFile.append() called outside of context manager'
            )
        if self.digest:
            self.digest.update(blob)
        tic = time.time()
        self.file.write(blob)
        toc = time.time()

        # timing
        new = len(blob)
        old = self.file.tell() - new
        self._update_speed(old, new, toc-tic)
        return self

    def write(self, blob: bytes) -> "IncompleteFile":
        return self.append(blob)

    def __add__(self, blob: bytes) -> "IncompleteFile":
        return self.append(blob)

    def _update_speed(self, total, nbytes, time):
        if not time:
            return
        self.last_speed = nbytes / time
        if self.mean_speed:
            self.mean_speed = total / self.mean_speed + time
            self.mean_speed = (total + nbytes) / self.mean_speed
        else:
            self.mean_speed = self.last_speed
