import os
import csv
import json
import logging
import nibabel
import numpy as np
from pathlib import Path

from braindataprep.utils.log import LoggingOutputSuppressor
from braindataprep.utils.path import fileparts

lg = logging.getLogger(__name__)


def nibabel_convert(
        src,
        dst,
        remove=False,
        inp_format=None,
        out_format=None,
        affine=None,
        makedirs=True,
):
    """
    Convert a volume between formats

    Parameters
    ----------
    src : str or Path
        Path to source volume
    dst : src or Path
        Path to destination volume
    remove : bool
        Delete source volume at the end
    inp_format : nibabel.Image subclass
        Input format (default: guess)
    out_format : nibabel.Image subclass
        Output format  (default: guess)
    affine : np.ndarray
        Orientation matrix (default: from input)
    """
    src = Path(src)
    dst = Path(dst)

    lg.info(f'write {dst.name}')

    if inp_format is None:
        f = nibabel.load(src)
    else:
        f = inp_format.load(src)
    if out_format is None:
        _, _, ext = fileparts(dst)
        if ext in ('.nii', '.nii.gz'):
            out_format = nibabel.Nifti1Image
        elif ext in ('.mgh', '.mgz'):
            out_format = nibabel.MGHImage
        elif ext in ('.img', '.hdr'):
            out_format = nibabel.AnalyzeImage
        else:
            raise ValueError('???')
    if affine is None:
        affine = f.affine
    if makedirs:
        dst.parent.mkdir(parents=True, exist_ok=True)
    with LoggingOutputSuppressor('nibabel.global'):
        nibabel.save(out_format(np.asarray(f.dataobj), affine, f.header), dst)
    if remove:
        for file in f.file_map.values():
            filename = Path(file.filename)
            if filename.exists():
                lg.info(f'remove {filename.name}')
                filename.unlink()


def read_json(src, **kwargs):
    """
    Read a JSON file

    Parameters
    ----------
    src : str or Path or file-like
        Input path

    Returns
    -------
    obj : dict
        Nested structure
    """
    if isinstance(src, (str, Path)):
        with open(src, 'rt') as fsrc:
            return read_json(fsrc, **kwargs)
    return json.load(src, **kwargs)


def write_json(src, dst, makedirs=True, **kwargs):
    """
    Write a BIDS json (indent = 2)

    Parameters
    ----------
    src : dict
        Serializable nested strucutre
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wt') as fdst:
            return write_json(src, fdst, **kwargs)
    kwargs.setdefault('indent', 2)
    json.dump(src, dst, **kwargs)


def copy_json(src, dst, makedirs=True, **kwargs):
    """
    Copy a JSON file, while ensuring that the output file follows our
    formatting convention (i.e., `indent=2`)

    Parameters
    ----------
    src : str or Path or file
        Input path
    dst : str or Path or file
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wt') as fdst:
            return copy_json(src, fdst, **kwargs, makedirs=False)
    if isinstance(src, (str, Path)):
        with open(src, 'rt') as fsrc:
            return copy_json(fsrc, dst, **kwargs, makedirs=False)
    kwargs.setdefault('indent', 2)
    json.dump(json.load(src), dst, **kwargs)


def write_tsv(src, dst, makedirs=True, **kwargs):
    r"""
    Write a BIDS tsv (delimiter = '\t', quoting=QUOTE_NONE)

    Parameters
    ----------
    src : list[list]
        A list of rows
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wt', newline='') as fdst:
            return write_tsv(src, fdst, makedirs=False, **kwargs)
    kwargs.setdefault('delimiter', '\t')
    kwargs.setdefault('quoting', csv.QUOTE_NONE)
    writer = csv.writer(dst, **kwargs)
    writer.writerows(src)


def write_from_buffer(src, dst, makedirs=True):
    """
    Write from an open buffer

    Parameters
    ----------
    src : io.BufferedReader or bytes
        An object with the `read()` method
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wb') as fdst:
            return write_from_buffer(src, fdst, makedirs=False)
    if isinstance(src, bytes):
        dst.write(src)
    else:
        dst.write(src.read())


def write_text(src, dst, makedirs=True):
    """
    Write a text file

    Parameters
    ----------
    src : str
        Some text
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        dst = Path(dst)
        lg.info(f'write {dst.name}')
        if makedirs:
            os.makedirs(dst.parent, exist_ok=True)
        with open(dst, 'wt') as fdst:
            return write_text(src, fdst, makedirs=False)
    dst.write(src)


def copy_from_buffer(src, dst, makedirs=True):
    """
    Write from a file or open buffer

    Parameters
    ----------
    src : str or Path or file-like
        Input path
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        lg.info(f'write {os.path.basename(dst)}')
        if makedirs:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, 'wb') as fdst:
            return copy_from_buffer(src, fdst, makedirs=False)
    if isinstance(src, (str, Path)):
        with open(src, 'rb') as fsrc:
            return copy_from_buffer(fsrc, dst, makedirs=False)
    if isinstance(src, bytes):
        dst.write(src)
    else:
        dst.write(src.read())
