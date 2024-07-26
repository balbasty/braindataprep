import os
from pathlib import Path
from typing import Optional, Literal, Iterable, Union
from humanize import naturalsize
from urllib.parse import urlparse

from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.utils.ui import human2bytes
from braindataprep.download import Downloader
from braindataprep.download import DownloadManager
from braindataprep.download import CHUNK_SIZE
from braindataprep.download import IfExists
from braindataprep.datasets.OASIS.I.command import oasis1


URLDATA = 'https://download.nrg.wustl.edu/data'
URLMETA = 'https://sites.wustl.edu/oasisbrains/files'
UID_META = '5708aa0a98d82080'
UID_RELI = '063c8642b909ee76'
UID_FACT = 'bcc7a002dfb104f4'
URLS = {
    'meta':
        f'{URLMETA}/2024/04/oasis_cross-sectional-{UID_META}.xlsx',
    'reliability':
        f'{URLMETA}/2024/04/oasis_cross-sectional-reliability-{UID_RELI}.xlsx',
    'facts':
        f'{URLMETA}/2024/03/oasis_cross-sectional_facts-{UID_FACT}.pdf',
    'raw': [
        URLDATA + '/oasis_cross-sectional_disc{:d}.tar.gz'.format(d)
        for d in range(1, 13)
    ],
    'fs': [
        URLDATA + '/oasis_cs_freesurfer_disc{:d}.tar.gz'.format(d)
        for d in range(1, 12)  # !!! no freesurfer disk 12
    ],
}

KeyChoice = Literal["raw", "fs", "meta", "reliability", "facts"]


@oasis1.command
def download(
    path: Optional[str] = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.__args__,
    discs: Iterable[int] = tuple(range(1, 13)),
    if_exists: IfExists.Choice = "skip",
    packet: Union[int, str] = naturalsize(CHUNK_SIZE),
    log: Optional[str] = None,
):
    """
    Download source data for the OASIS-I dataset.

    **Possible keys:**
    * **raw**          All the raw imaging data
    * **fs**           Data processed with FreeSurfer
    * **meta**         Metadata
    * **reliability**  Repeatability measures data sheet
    * **facts**        Fact sheet

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `OASIS-1` folder will be created.
    keys : [list of] {"raw", "fs", "meta", "reliability", "facts"}
        Data categories to download
    discs : [list of] {1..12}
        Discs to download
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
    discs = set(discs or list(range(1, 13)))
    src = path / 'OASIS-1' / 'sourcedata'
    downloaders = []
    for key, URL in URLS.items():
        if key not in keys:
            continue
        if key in ('raw', 'fs'):
            for disc in discs:
                if disc >= len(URL):
                    continue
                URL1 = URL[disc-1]
                downloaders.append(Downloader(
                    URL1,  src / Path(urlparse(URL1).path).name,
                    ifexists=if_exists,
                    chunk_size=human2bytes(packet),
                ))
        else:
            if key == 'meta':
                basename = 'oasis_cross-sectional.xlsx'
            elif key == 'reliability':
                basename = 'oasis_cross-sectional-reliability.xlsx'
            elif key == 'facts':
                basename = 'oasis_cross-sectional_facts.pdf'
            else:
                basename = os.path.basename(URL)
            downloaders.append(Downloader(
                URL,  src / basename,
                ifexists=if_exists,
                chunk_size=human2bytes(packet),
            ))
    DownloadManager(downloaders).run()
