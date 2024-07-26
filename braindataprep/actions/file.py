import time
from pathlib import Path
from shutil import rmtree, copy2
from fasteners import InterProcessReaderWriterLock
from typing import IO, Tuple, Iterable
from logging import getLogger

lg = getLogger(__name__)


class File:
    """
    An object that represents a file in the tree.

    It can be in the process of being read, or in the process of
    being written. Reader/Writer locks ensure that a file is not
    overwritten while it is being read. During writing, a temporary
    file is created first, and only renamed to its final name if
    everything completed properly.

    ```python
    # Open file object for reading
    with File(filename) as file_ref:
        # no protection at this point
        with file_ref.open("rb") as f:
            # read protection enabled
            dat = f.read()

    # Alternative syntax
    with File(filename, "rb") as file_ref:
        # read protection enabled
        with file_ref.open() as f:
            dat = f.read()

    # Open file object for writing
    with File(filename) as file_ref:
        # no protection at this point
        with file_ref.open("wb") as f:
            # write protection enabled
            f.write(dat)

    # Alternative syntax
    with File(filename, "wb") as file_ref:
        # write protection enabled
        with file_ref.open() as f:
            f.write(dat)

    # Protect file for reading (but do not open a file-object)
    with File(filename, "r") as file_ref:
        # read protection enabled
        subroutine_read(file_ref.safename)

    # Protect file for writing (but do not open a file-object)
    with File(filename, "w") as file_ref:
        # write protection enabled
        subroutine_write(file_ref.safename)
    ```

    # Modes

    * `'r'` : open for reading
    * `'w'` : open for writing, truncating the file first
    * `'a'` : open for writing, appending to the end of file if it exists
    * `'b'` : binary mode
    * `'t'` : text mode
    * `'+'` : open for updating (reading and writing)

    This means that:

    | `mode` | truncates | opens at | `read`  | `write` |
    | ------ | --------- | -------- | ------- | ------- |
    | `'r'`  |     ✗     | start    |    ✓    |    ✗    |
    | `'w'`  |     ✓     | start    |    ✗    |    ✓    |
    | `'a'`  |     ✗     | end      |    ✗    |    ✓    |
    | `'r+'` |     ✗     | start    |    ✓    |    ✓    |
    | `'w+'` |     ✓     | start    |    ✓    |    ✓    |

    """

    # Derived from `dandi.download.DownloadDirectory`
    # https://github.com/dandi/dandi-cli/blob/master/dandi/download.py
    # Apache License Version 2.0

    def __init__(
            self,
            filename: str | Path,
            mode: str | None = None,
    ) -> None:
        """
        Parameters
        ----------
        filename : str | Path
            Output filename
        mode : {'r', 'w', 'x', 'a', 'b', 't', '+'} | None
            Protection & opening mode.
            By default, no protection is applied until a file-object is
            opened, in which case approprite protection is applied.
        """
        # assign
        self.mode = mode
        self.filename: Path = Path(filename)
        self.tempdir: Path = self.filename.with_name(
            self.filename.name + '.tmp'
        )
        self.tempname: Path = self.tempdir / self.filename.name
        self.lockname: Path = self.tempdir / 'lock'
        self.safename = None
        self.lock: InterProcessReaderWriterLock = None
        self.file: IO[bytes] = None
        self.writable = None
        self.readable = None

    def open(self, mode: str | None = None, **kwargs) -> "OpenedFile":
        r"""
        Open the file and return a file-like context

        Parameters
        ----------
        mode : {'r', 'w', 'x', 'a', 'b', 't', '+'} | None
            Protection & opening mode.
            By default: same as `self.mode`, or 'rt' if `self.mode=None`.

        Other Parameters
        ----------------
        buffering: int, default=-1
            * `0`    : switch buffering off (only allowed in binary mode)
            * `1`    : line buffering (only usable when writing in text mode)
            *  `> 1` : size in bytes of a fixed-size chunk buffer.
        encoding: str, optional
            Name of (text-mode) encoding/decoding coded.
            Can be any codec supported by python.
        errors : {'strict', 'ignore', 'replace', 'backslashreplace',
                  'surrogateescape', 'xmlcharrefreplace', 'namereplace'}
            How to handle errors
        newline : {None, '', '\n', '\r', '\r\n'}
            How to parse newline characters from the stream (text mode)

        Returns
        -------
        OpenedFile
        """
        if self.lock is None:
            raise ValueError('File.open() called outside of context manager')
        mode = mode or self.mode or 'rb'
        return OpenedFile(self, mode, **kwargs)

    def __enter__(self):

        # Remove existing file
        self.tempdir.mkdir(parents=True, exist_ok=True)
        if self.tempname.is_dir():
            rmtree(self.tempname)
        else:
            self.tempname.unlink(missing_ok=True)

        if self.mode:
            # Acquire lock
            self.lock = InterProcessReaderWriterLock(str(self.lockname))

            mode = self.mode
            self.writable = 'w' in mode or 'a' in mode or '+' in mode
            self.readable = 'r' in mode or '+' in mode

            if (
                self.writable and
                not (self.lock.acquire_write_lock(blocking=False))
            ):
                raise RuntimeError(
                    f'Could not acquire write lock for {self.filename}'
                )

            elif (
                self.readable and
                not (self.lock.acquire_read_lock(blocking=False))
            ):
                raise RuntimeError(
                    f'Could not acquire read lock for {self.filename}'
                )

        if self.writable:
            self.safename = self.tempname
        else:
            self.safename = self.filename

        # Copy file into temp
        if 'a' in mode or ('r' in mode and '+' in mode):
            if self.filename.exists():
                copy2(self.filename, self.tempname)

        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Rename temporary filename to output filename
        # Note that we only rename the file to its final name and
        # remove temporary file if the download was succesful (i.e.
        # the context was not interrupted by an exception)
        try:
            if exc_type is None and self.tempname.exists():
                try:
                    self.tempname.replace(self.filename)
                except IsADirectoryError:
                    rmtree(self.filename)
                    self.tempname.replace(self.filename)
        finally:
            # Release lock and delete existing files
            if self.lock is not None:
                if self.writable:
                    try:
                        self.lock.release_write_lock()
                    except RuntimeError:
                        # we were not owning a write lock
                        pass
                elif self.readable:
                    try:
                        self.lock.release_read_lock()
                    except RuntimeError:
                        # we were not owning a read lock
                        pass
            if exc_type is None:
                rmtree(self.tempdir)
            self.lock = None
            self.safename = None
            self.writable = None
            self.readable = None


class Files:
    """A collection of files"""

    def __init__(self, *files: File):
        """
        Parameters
        ----------
        *files : File
            A series of `File`
        """
        self.files = files
        self._unopened = list(files)
        self._opened = []

    def __enter__(self):
        while self._unopened:
            file = self._unopened.pop(0)
            file.__enter__()
            self._opened.append(file)

    def __exit__(self, exc_type, exc_val, exc_tb):
        while self._opened:
            file = self._opened.pop(-1)
            file.__exit__(exc_type, exc_val, exc_tb)
            self._unopened.insert(0, file)

    def open(self, *a, **k) -> "OpenedFile" | Tuple["OpenedFile"]:
        r"""
        Open the file(s) and return file-like context(s)

        Parameters
        ----------
        mode : {'r', 'w', 'x', 'a', 'b', 't', '+'} | None
            Protection & opening mode.
            By default: same as `self.mode`, or 'rt' if `self.mode=None`.

        Other Parameters
        ----------------
        buffering: int, default=-1
            * `0`    : switch buffering off (only allowed in binary mode)
            * `1`    : line buffering (only usable when writing in text mode)
            *  `> 1` : size in bytes of a fixed-size chunk buffer.
        encoding: str, optional
            Name of (text-mode) encoding/decoding coded.
            Can be any codec supported by python.
        errors : {'strict', 'ignore', 'replace', 'backslashreplace',
                  'surrogateescape', 'xmlcharrefreplace', 'namereplace'}
            How to handle errors
        newline : {None, '', '\n', '\r', '\r\n'}
            How to parse newline characters from the stream (text mode)

        Returns
        -------
        *files : OpenedFile
            A series of file objects.
        """
        filobjs = tuple(file.open(*a, **k) for file in self.files)
        if len(filobjs) == 0:
            return None
        if len(filobjs) == 1:
            return filobjs[0]
        else:
            return filobjs


class FileObjMixin:
    """Implements logic common to all file objects

    Assumes the class has attributes:
    * fileobj: IO[bytes]
    * total_read: int
    * last_read_speed: float
    * mean_read_speed: float
    * total_write: int
    * last_write_speed: float
    * mean_write_speed: float
    """

    def error_if_notincontext(self, name: str) -> None:
        if self.fileobj is None:
            raise ValueError(
                f'FileObj.{name}() called outside of context manager'
            )

    def tell(self) -> int:
        self.error_if_notincontext('write')
        return self.fileobj.tell()

    def seek(self, *a, **k):
        self.error_if_notincontext('write')
        return self.fileobj.seek(*a, **k)

    def write(self, blob: bytes | str) -> "FileObjMixin":
        self.error_if_notincontext('write')
        tic = time.time()
        self.fileobj.write(blob)
        toc = time.time()
        self._update_write_speed(len(blob), toc-tic)
        return self

    def read(self, nbytes: int | None = None) -> bytes | str:
        self.error_if_notincontext('read')
        tic = time.time()
        blob = self.fileobj.read(nbytes)
        toc = time.time()
        self._update_read_speed(len(blob), toc-tic)
        return blob

    def readline(self) -> str:
        self.error_if_notincontext('readline')
        tic = time.time()
        line = self.fileobj.readline()
        toc = time.time()
        self._update_read_speed(len(line), toc-tic)
        return line

    def readlines(self, nlines: int) -> Iterable[str]:
        self.error_if_notincontext('readlines')
        tic = time.time()
        line = self.fileobj.readlines(nlines)
        toc = time.time()
        self._update_read_speed(len(line), toc-tic)
        return line

    def __iter__(self):
        self.error_if_notincontext('__iter__')
        lines = iter(self.fileobj)
        while True:
            try:
                tic = time.time()
                line = next(lines)
                toc = time.time()
                self._update_read_speed(len(line), toc-tic)
                yield line
            except StopIteration:
                return

    def __next__(self) -> str:
        self.error_if_notincontext('__next__')
        tic = time.time()
        line = self.fileobj.__next__()
        toc = time.time()
        self._update_read_speed(len(line), toc-tic)
        return line

    def append(self, blob: bytes | str) -> "FileObjMixin":
        return self.write(blob)

    def __add__(self, blob: bytes | str) -> "FileObjMixin":
        return self.append(blob)

    def _update_read_speed(self, nbytes: int, time: float) -> None:
        if time == 0:
            # too fast for proper timing
            return
        self.last_read_speed = nbytes / time
        if self.mean_read_speed:
            self.mean_read_speed = self.total_read / self.mean_read_speed
            self.mean_read_speed += time
            self.total_read += nbytes
            self.mean_read_speed = self.total_read / self.mean_read_speed
        else:
            self.mean_read_speed = self.last_read_speed
            self.total_read += nbytes

    def _update_write_speed(self, nbytes: int, time: float) -> None:
        if time == 0:
            # too fast for proper timing
            return
        self.last_write_speed = nbytes / time
        if self.mean_write_speed:
            self.mean_write_speed = self.total_write / self.mean_write_speed
            self.mean_write_speed += time
            self.total_write += nbytes
            self.mean_write_speed = self.total_write / self.mean_write_speed
        else:
            self.mean_write_speed = self.last_write_speed
            self.total_write += nbytes


class FileObj(FileObjMixin, File):
    """
    An opened file object that represents a file in the tree.

    It can be in the process of being read, or in the process of
    being written. Reader/Writer locks ensure that a file is not
    overwritten while it is being read. During writing, a temporary
    file is created first, and only renamed to its final name if
    everything completed properly.

    ```python
    # Open file object for reading
    with FileObj(filename, "rb") as f:
        # read protection enabled
        dat = f.read()

    # Open file object for writing
    with FileObj(filename, "wb") as f:
        # write protection enabled
        f.write(dat)
    ```

    # Modes

    * `'r'` : open for reading
    * `'w'` : open for writing, truncating the file first
    * `'a'` : open for writing, appending to the end of file if it exists
    * `'b'` : binary mode
    * `'t'` : text mode
    * `'+'` : open for updating (reading and writing)

    This means that:

    | `mode` | truncates | opens at | `read` | `write`|
    | ------ | --------- | -------- | ------ | ------ |
    | `'r'`  |     ✗     | start    |   ✓    |   ✗    |
    | `'w'`  |     ✓     | start    |   ✗    |   ✓    |
    | `'a'`  |     ✗     | end      |   ✗    |   ✓    |
    | `'r+'` |     ✗     | start    |   ✓    |   ✓    |
    | `'w+'` |     ✓     | start    |   ✓    |   ✓    |

    """

    def __init__(
            self,
            filename: str | Path,
            mode: str | None = 'rb',
    ):
        if mode is None:
            raise ValueError('mode must be provided')
        super().__init__(filename, mode)
        self.fileobj = None
        self.total_read = 0
        self.last_read_speed = 0
        self.mean_read_speed = 0
        self.total_write = 0
        self.last_write_speed = 0
        self.mean_write_speed = 0

    def __enter__(self) -> "FileObj":
        super().__enter__()
        self.tempdir.mkdir(parents=True, exist_ok=True)
        self.fileobj = self.safename.open(self.mode)
        self.total_read = 0
        self.total_write = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        assert self.file is not None
        self.fileobj.close()
        self.fileobj = None
        super().__exit__(self, exc_type, exc_val, exc_tb)


class OpenedFile(FileObjMixin):
    """
    File object returned by File.open().

    It is slightly different from `actions.FileObj` as it defers
    temporary file cleaning and (some) lock acquisition/release to
    the calling `File` object.

    !!! warning
        This object **should not** be created by a user.
        It should **only** be created inside `File.open()`.
    """

    def __init__(self, file: File, mode: str | None) -> None:
        # checks
        if mode is None:
            mode = self.file.mode
        if mode is None:
            raise ValueError('A mode must be provided')
        # assign
        self.file = file
        self.mode = mode
        self.lock = None
        self.fileobj = None
        self.writable = None
        self.readable = None
        self.total_read = 0
        self.last_read_speed = 0
        self.mean_read_speed = 0
        self.total_write = 0
        self.last_write_speed = 0
        self.mean_write_speed = 0

    def __enter__(self) -> "OpenedFile":
        # Acquire lock
        mode = self.mode
        if mode == 'b' or mode == 't':
            mode = 'r' + mode

        self.writable = 'w' in mode or 'a' in mode or '+' in mode
        self.readable = 'r' in mode or '+' in mode

        if self.writable and self.file.writable is False:
            raise ValueError(
                'File was not opened in write mode, '
                'so file object cannot be opened in write mode.')
        if self.readable and self.file.readable is False:
            raise ValueError(
                'File was not opened in read mode, '
                'so file object cannot be opened in read mode.')

        if self.file.lock is None:
            self.lock = InterProcessReaderWriterLock(str(self.file.lockname))
            if (
                self.writable and
                not self.lock.acquire_write_lock(blocking=False)
            ):
                raise RuntimeError(
                    f'Could not acquire write lock for {self.file.filename}'
                )
            elif (
                self.readable and
                not self.lock.acquire_read_lock(blocking=False)
            ):
                raise RuntimeError(
                    f'Could not acquire read lock for {self.file.filename}'
                )

            # Copy file into temp
            if 'a' in mode or ('r' in mode and '+' in mode):
                if self.file.filename.exists():
                    copy2(self.file.filename, self.file.tempname)

        self.fileobj = self.file.safename.open(mode)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        # Close file object
        assert self.file is not None
        self.fileobj.close()
        self.fileobj = None
        # Release lock
        if self.lock is not None:
            if self.writable:
                try:
                    self.lock.release_write_lock()
                except RuntimeError:
                    # we were not owning a write lock
                    pass
            elif self.readable:
                try:
                    self.lock.release_read_lock()
                except RuntimeError:
                    # we were not owning a read lock
                    pass
        self.lock = None
