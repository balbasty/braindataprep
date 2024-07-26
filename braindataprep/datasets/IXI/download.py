from pathlib import Path
from urllib.parse import urlparse
from humanize import naturalsize
from typing import Literal, Iterable

from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.utils.ui import human2bytes
from braindataprep.download import Downloader
from braindataprep.download import DownloadManager
from braindataprep.download import CHUNK_SIZE
from braindataprep.download import IfExists
from braindataprep.datasets.IXI.command import ixi


URLBASE = 'https://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI'
URLS = {
    'meta': [f'{URLBASE}/IXI.xls'],
    'T1w': [f'{URLBASE}/IXI-T1.tar'],
    'T2w': [f'{URLBASE}/IXI-T2.tar'],
    'PDw': [f'{URLBASE}/IXI-PD.tar'],
    'angio': [f'{URLBASE}/IXI-MRA.tar'],
    'dwi': [
        f'{URLBASE}/IXI-DTI.tar',
        f'{URLBASE}/bvecs.txt',
        f'{URLBASE}/bvals.txt',
    ],
}

KeyChoice = Literal["T1w", "T2w", "PDw", "angio", "dwi", "meta"]


@ixi.command
def download(
    path: str | None = None,
    *,
    keys: Iterable[KeyChoice] = tuple(URLS.keys()),
    if_exists: IfExists.Choice = "skip",
    packet: int | str = naturalsize(CHUNK_SIZE),
    log: str | None = None,
) -> None:
    """
    Download source data for the IXI dataset.

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `IXI` folder will be created.
    keys : [list of] {"T1w", "T2w", "PDw", "angio", "dwi", "meta"}
        Modalities to download
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    packet : int
        Packet size to download, in bytes
    log : str
        Path to log file
    """
    setup_filelog(log)
    keys = keys or URLS.keys()
    keys = list({key: None for key in keys}.keys())  # remove duplicates
    path: Path = Path(get_tree_path(path))
    src = path / 'IXI' / 'sourcedata'
    src.mkdir(parents=True, exist_ok=True)
    DownloadManager(
        Downloader(
            url,  src / Path(urlparse(url).path).name,
            ifexists=if_exists,
            chunk_size=human2bytes(packet),
        )
        for key in keys
        for url in URLS[key]
    ).run()
