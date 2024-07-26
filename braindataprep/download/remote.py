import requests
import time
from typing import Callable
from email.utils import parsedate_to_datetime
from urllib.parse import urlparse, ParseResult

from braindataprep.download.constants import CHUNK_SIZE


class RemoteFile:
    """
    This object represents a remote file, whose bytes are downloaded.
    It is used as a context manager.

    ```python
    with RemoteFile(url) as f:
        obj = b''
        for chunk in f:
            obj += chunk
    ```
    """

    REDIRECTION = (300, 301, 302, 303, 307, 308)

    def __init__(
            self,
            url: str | ParseResult,
            session: requests.Session | None = None,
            auth: Callable[[requests.Session], None] = None,
            chunk_size: int = CHUNK_SIZE,
            offset: int = 0,
    ):
        """
        Parameters
        ----------
        url : str | ParseResult
            Remote URL
        session : Session
            Opened session
        auth : callable[Session]
            Authentification function
        chunk_size : int
            Number of bytes to read at once
        offset : int
            Number of bytes to skip
        """
        if not isinstance(url, ParseResult):
            url = urlparse(url)
        self.url = url
        self.session = session
        self.session_is_mine = session is not None
        self.auth = auth or (lambda x: None)
        self.chunk_size = chunk_size
        self.has_range = self._check_has_range()
        self.offset = offset
        self.response = None
        self.iterator = None
        self.buffer = None
        self.last_speed = None
        self.mean_speed = 0

    def _check_has_range(self):
        answer = False
        if self.session is None:
            self.session = requests.Session()
            self.auth(self.session)
        try:
            h = {'Range': 'bytes=0-0'}
            r = self.session.head(self.url.geturl(), headers=h)
            if r.status_code in self.REDIRECTION:
                self.url = urlparse(r.headers['Location'])
                r = self.session.head(self.url.geturl(), headers=h)
            if r.status_code not in (200, 206) and self.auth:
                self.auth(self.session)
                r = self.session.head(self.url.geturl(), headers=h)
            answer = (r.status_code == 206)
        finally:
            if not self.session_is_mine:
                self.session.close()
                self.session = None
            return answer

    @property
    def size(self):
        """Try to guess the file size from remote"""
        if self.response:
            if 'Content-Range' in self.response.headers:
                return int(
                    self.response.headers['Content-Range'].split('/')[-1]
                )
            elif 'Content-Length' in self.response.headers:
                return int(self.response.headers['Content-Length'])
            else:
                return None
        else:
            size = None
            if self.session is None:
                self.session = requests.Session()
                self.auth(self.session)
            try:
                r = self.session.head(self.url.geturl())
                if r.status_code in self.REDIRECTION:
                    self.url = urlparse(r.headers['Location'])
                    r = self.session.head(self.url.geturl())
                if r.status_code != 200 and self.auth:
                    self.auth(self.session)
                    r = self.session.head(self.url.geturl())
                if r.status_code == 200 and 'Content-Length' in r.headers:
                    size = int(r.headers['Content-Length'])
            finally:
                if not self.session_is_mine:
                    self.session.close()
                    self.session = None
                return size

    @property
    def mtime(self):
        """Try to guess the "last-modified" time from remote"""
        if self.response:
            if 'Last-Modified' in self.response.headers:
                return parsedate_to_datetime(
                    self.response.headers['Last-Modified']
                )
            else:
                return None
        else:
            mtime = None
            if self.session is None:
                self.session = requests.Session()
                self.auth(self.session)
            try:
                r = self.session.head(self.url.geturl())
                if r.status_code in self.REDIRECTION:
                    self.url = urlparse(r.headers['Location'])
                    r = self.session.head(self.url.geturl())
                if r.status_code != 200 and self.auth:
                    self.auth(self.session)
                    r = self.session.head(self.url.geturl())
                if r.status_code == 200 and 'Last-Modified' in r.headers:
                    mtime = parsedate_to_datetime(r.headers['Last-Modified'])
            finally:
                if not self.session_is_mine:
                    self.session.close()
                    self.session = None
                return mtime

    def __enter__(self):
        # open session
        if self.session is None:
            self.session = requests.Session()
            self.auth(self.session)
        # open content streamer
        h = {}
        if self.offset and self.has_range:
            h['Range'] = f'bytes={self.offset}-'
        self.response = self.session.get(
            self.url.geturl(), stream=True, headers=h
        )
        if self.response.status_code in self.REDIRECTION:
            self.url = urlparse(self.response.headers['Location'])
            self.response = self.session.get(
                self.url.geturl(), stream=True, headers=h
            )
        self.response.__enter__()
        # get content chunk iterator
        self.iterator = iter(self.response.iter_content(self.chunk_size))
        # skip offset if range not available
        if self.offset and not self.has_range:
            self._skip(self.offset)
        else:
            self.total = 0
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.iterator = None
        self.response.__exit__(exc_type, exc_val, exc_tb)
        self.response = None
        self.buffer = None
        if not self.session_is_mine:
            self.session.close()
            self.session = None

    def _skip(self, nbytes):
        if self.buffer is None:
            self.buffer = b''
        self.total = 0
        try:
            while self.total < nbytes:
                tic = time.time()
                chunk = next(self.iterator)
                toc = time.time()
                # timing (total must be increased _after_ update speed)
                self._update_speed(len(chunk), toc-tic)
                self.total += len(chunk)
        except StopIteration:
            pass
        self.buffer = self.buffer[nbytes:]
        if len(self.buffer) == 0:
            self.buffer = None

    def __iter__(self):
        if self.buffer is not None:
            yield self.buffer

        try:
            while True:
                tic = time.time()
                chunk = next(self.iterator)
                toc = time.time()
                # timing (total must be increased _after_ update speed)
                self._update_speed(len(chunk), toc-tic)
                self.total += len(chunk)
                # yield
                yield chunk

        except StopIteration:
            return

    def _update_speed(self, nbytes, time):
        if not time:
            return
        self.last_speed = nbytes / time
        if self.mean_speed:
            self.mean_speed = self.total / self.mean_speed + time
            self.mean_speed = (self.total + nbytes) / self.mean_speed
        else:
            self.mean_speed = self.last_speed
