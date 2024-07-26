from pathlib import Path
from collections import Counter
from logging import getLogger
from typing import Literal, Iterable

from braindataprep.pyout import LogSafeTabular, get_style
from braindataprep.download.downloader import IfExists
from braindataprep.download.downloader import Downloader

lg = getLogger(__name__)


class DownloadManager:
    """
    A class that manages is list of downloads.

    It runs them one at a time and display their status in a table.

    ```python
    manager = DownloadManager(
        Downloader(url1, fname1),
        Downloader(url2, fname2),
        Downloader(url3, fname3),
    )
    manager.run()
    ```
    """

    def __init__(
            self,
            downloaders: Iterable[Downloader],
            ifexists: IfExists.Choice | None = None,
            on_error: Literal["yield", "raise"] = "yield",
            path: Literal["name", "full", "abs", "short"] = "name",
            jobs: int = 1,
    ):
        """
        Parameters
        ----------
        *downloaders : Downloader
            A list of downloaders

        Other Parameters
        ----------------
        ifexists : {'error', 'skip', 'overwrite', 'different', 'refresh', None}
            Behaviour if destination file already exists
        on_error : {"yield", "raise"}
            Whether to raise an error a yield a status when an exception
            is encountered in a download
        path : {"name", "full", "abs", "short"}
            Which version of the path to display
            * "name"  : file name only
            * "full"  : full path (as stored in downloader)
            * "abs"   : absolute path
            * "short" : hide common prefix
        jobs : int
            Number of `pyout` jobs used for printing
        """
        self.downloaders = downloaders

        self.ifexists = IfExists.from_any(ifexists)
        self.on_error = on_error
        self.path = path

        rec_fields = (
            "path",
            "size",
            "done",
            "done%",
            "checksum",
            "dspeed",
            "wspeed",
            "status",
            "message",
        )
        self.out = LogSafeTabular(
            style=get_style(hide_if_missing=False),
            columns=rec_fields,
            max_workers=jobs,
        )

    def run(self):
        """Run all downloads"""
        guard = {'yield': self.guard, 'raise': lambda x: x}[self.on_error]

        if self.path[0] == 's':
            # Shorten path, but we need to access all downloaders wich
            # might be slow is the input is a looooong generator
            self.downloaders = list(self.downloaders)
            paths = self.shortpath([dl.dst for dl in self.downloaders])

            with self.out:
                with IfExists(self.ifexists):
                    for path, downloader in zip(paths, self.downloaders):
                        for status in guard(downloader):
                            self.out({"path": path, **status})

        else:
            # Just yield from the generator
            for downloader in self.downloaders:
                path = str(self.repath(downloader.dst))
                for status in guard(downloader):
                    self.out({"path": path, **status})

    def guard(self, downloader):
        try:
            yield from downloader
        except Exception as exc:
            lg.exception(
                "Caught while downloading %s:", downloader.dst
            )
            yield {
                "status": "error",
                "message": str(exc.__class__.__name__),
            }

    def shortpath(self, paths):
        if len(paths) == 1:
            # fallback to mode "name"
            return [path.name for path in paths]
        common = self.commonprefix(*paths)
        if common is None:
            # fallback to mode "full"
            return paths
        return [path.relative_to(Path(common)) for path in paths]

    def repath(self, path):
        mode = self.path[0].lower()
        if mode == "a":  # abs
            return path.absolute()
        if mode == "n":  # name
            return path.name
        if mode == "f":  # full
            return path
        assert False

    def commonprefix(self, *paths):
        """Common prefix of given paths"""
        # https://gist.github.com/chrono-meter/7e47528a3f902c9ade7e0cc442394d08
        counter = Counter()

        for path in paths:
            counter.update([path])
            counter.update(path.parents)

        try:
            return sorted(
                (x for x, count in counter.items() if count >= len(paths)),
                key=lambda x: len(str(x))
            )[-1]
        except LookupError:
            return None
