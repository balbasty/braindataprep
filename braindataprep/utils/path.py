import os
from pathlib import Path


def with_bids_suffix(path: str | Path, suffix) -> str | Path:
    """
    Change the file suffix of a BIDS file.
    Contrary to `Path.with_suffix`, all suffixes are removed and replaced.
    """
    T = type(path)
    path = Path(path)
    path = path.with_name(path.name.split('.')[0] + suffix)
    return T(path)


def get_tree_path(path: str | Path | None = None) -> Path:
    """
    If path not set, return the default path stored in the environment
    variable `BDP_PATH`. If unset, return current folder.
    """
    return Path(path or os.environ.get('BDP_PATH', '.'))


def fileparts(fname):
    """Compute parts from path

    Parameters
    ----------
    fname : str or Path
        Path

    Returns
    -------
    dirname : str or Path
        Directory path
    basename : str
        File name without extension
    ext : str
        Extension
    """
    if isinstance(fname, Path):
        dirname = fname.parent
        basename = fname.name
    else:
        dirname = os.path.dirname(fname)
        basename = os.path.basename(fname)
    basename, ext = os.path.splitext(basename)
    if ext in ('.gz', '.bz2'):
        compression = ext
        basename, ext = os.path.splitext(basename)
        ext += compression
    return dirname, basename, ext
