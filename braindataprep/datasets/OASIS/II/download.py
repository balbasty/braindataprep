from pathlib import Path
from typing import Literal, Iterable
from urllib.parse import urlparse
from humanize import naturalsize

from braindataprep.utils.ui import human2bytes
from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.download import DownloadManager
from braindataprep.download import Downloader
from braindataprep.download import IfExists
from braindataprep.download import CHUNK_SIZE
from braindataprep.datasets.OASIS.II.command import oasis2


URLBASE = 'https://download.nrg.wustl.edu/data'
OASISBASE = 'https://sites.wustl.edu/oasisbrains/files/2024/03/'
URLS = {
    'raw': [
        f'{URLBASE}/OAS2_RAW_PART1.tar.gz',
        f'{URLBASE}/OAS2_RAW_PART2.tar.gz',
    ],
    'meta': [
        f'{OASISBASE}/oasis_longitudinal_demographics-8d83e569fa2e2d30.xlsx',
    ],
}

KeyChoice = Literal["raw", "meta"]


@oasis2.command
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.__args__,
    parts: Iterable[int] = (1, 2),
    if_exists: IfExists.Choice = "skip",
    packet: int | str = naturalsize(CHUNK_SIZE),
    log: str | None = None,
):
    """
    Download source data for the OASIS-II dataset.

    **Possible keys:**
    * **raw**          All the raw imaging data
    * **fs**           Data processed with FreeSurfer
    * **meta**         Metadata
    * **reliability**  Repeatability measures data sheet
    * **facts**        Fact sheet

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `OASIS-2` folder will be created.
    keys : [list of] {"raw", "meta"}
        Data categories to download
    parts : [list of] {1, 2}
        Parts to download
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    packet : int
        Packet size to download, in bytes
    log : str
        Path to log file

    """
    setup_filelog(log)
    path = Path(get_tree_path(path))
    keys = set(keys or URLS.keys())
    parts = set(parts or (1, 2))
    src = path / 'OASIS-2' / 'sourcedata'
    downloaders = []
    if 'raw' in keys:
        for part in parts:
            URL = URLS['raw'][part-1]
            downloaders.append(Downloader(
                URL,  src / Path(urlparse(URL).path).name,
                ifexists=if_exists,
                chunk_size=human2bytes(packet),
            ))
    if 'meta' in keys:
        URL = URLS['meta'][0]
        basename = 'oasis_longitudinal_demographics.xlsx'
        downloaders.append(Downloader(
            URL,  src / basename,
            ifexists=if_exists,
            chunk_size=human2bytes(packet),
        ))
    DownloadManager(downloaders).run()
