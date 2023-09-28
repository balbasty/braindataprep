import requests
import os
import time
import nibabel
import json
import csv
import logging
from pathlib import Path
import numpy as np


def get_tree_path(path=None):
    if not path:
        path = os.environ.get(
            'BDP_PATH',
            '/autofs/space/pade_004/users/yb947/data4'
        )
    return path


def download_file(src, dst=None, packet_size=1024, makedirs=True, session=None,
                  **kwargs):
    """
    Download a file

    Parameters
    ----------
    url : str
        File URL.
    path : str or Path or  file-like
        Output path.

    Other Parameters
    ----------------
    packet_size : int
        Download packets of this size.
        If None, download the entire file at once.
    makedirs : bool, default=True
        Create all directories needs to write the file

    Returns
    -------
    path : str
        Output path.
    """
    if dst is None:
        dst = os.path.join('.', os.path.basename(src))
    if isinstance(dst, (str, Path)):
        if os.path.isdir(dst):
            dst = os.path.join(dst, os.path.basename(src))
        if makedirs:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        kwargs['fname'] = dst
        logging.info(f'download {os.path.basename(dst)}')
        with open(dst, 'wb') as fdst:
            return download_file(src, fdst, packet_size=packet_size,
                                 session=session, **kwargs)

    if not session:
        session = requests.Session()

    with session.get(src, stream=True) as finp:
        total_size = finp.headers.get("Content-Length", None)
        if total_size:
            total_size = int(total_size)
        else:
            total_size = None

        if packet_size:
            packet_sum = 0
            tic = time.time()
            for packet in finp.iter_content(packet_size):
                if len(packet) == 0:
                    continue
                tac = time.time()
                dst.write(packet)
                toc = time.time()
                packet_sum += len(packet)
                show_download_progress(
                    packet_sum, total_size,
                    time=(len(packet), tic, tac, toc)
                )
                tic = time.time()
            if total_size and (packet_sum != total_size):
                print('  INCOMPLETE')
            else:
                print('  COMPLETE')
        else:
            dst.write(finp.content)
    return kwargs.pop('fname', None)


def show_download_progress(size, total_size=None, time=None, end='\r'):
    size, size_unit = round_bytes(size)
    print(f'{end}{size:7.3f} {size_unit}', end='')
    if total_size:
        total_size, total_unit = round_bytes(total_size)
        print(f' / {total_size:7.3f} {total_unit}', end='')
    if time:
        packet_size, tic, tac, toc = time
        tb, tb_unit = round_bytes(packet_size / max(toc - tic, 1e-9))
        db, db_unit = round_bytes(packet_size / max(tac - tic, 1e-9))
        wb, wb_unit = round_bytes(packet_size / max(toc - tac, 1e-9))
        print(f' [dowload: {db:7.3f} {db_unit}/s'
              f' | write: {wb:7.3f} {wb_unit}/s'
              f' | total: {tb:7.3f} {tb_unit}/s]', end='')


def round_bytes(x):
    """
    Convert a number of bytes to the unit of correct magnitude

    Parameters
    ----------
    x : int
        Number of bytes

    Returns
    -------
    x : float
        Number of [unit]
    unit : {'B', 'KB', 'MB', 'GB', 'TB'}
        Unit of returned value
    """
    if x < 1024:
        return x, 'B'
    elif x < 1024**2:
        return x / 1024, 'KB'
    elif x < 1024**3:
        return x / 1024**2, 'MB'
    elif x < 1024**4:
        return x / 1024**3, 'GB'
    else:
        return x / 1024**4, 'TB'


def fileparts(fname):
    """Compute parts from path

    Parameters
    ----------
    fname : str
        Path

    Returns
    -------
    dirname : str
        Directory path
    basename : str
        File name without extension
    ext : str
        Extension
    """
    dirname = os.path.dirname(fname)
    basename = os.path.basename(fname)
    basename, ext = os.path.splitext(basename)
    if ext in ('.gz', '.bz2'):
        compression = ext
        basename, ext = os.path.splitext(basename)
        ext += compression
    return dirname, basename, ext


class LoggingOutputSuppressor:
    """Context manager to prevent global logger from printing"""

    def __init__(self, logger) -> None:
        self.logger = logger

    def __enter__(self):
        logger = self.logger
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        self.orig_handlers = logger.handlers
        for handler in self.orig_handlers:
            logger.removeHandler(handler)

    def __exit__(self, exc, value, tb):
        logger = self.logger
        if isinstance(logger, str):
            logger = logging.getLogger(logger)
        for handler in self.orig_handlers:
            logger.addHandler(handler)


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
    src : str
        Path to source volume
    dst : src
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
    logging.info(f'write {os.path.basename(dst)}')

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
        os.makedirs(os.path.dirname(dst), exist_ok=True)
    with LoggingOutputSuppressor('nibabel.global'):
        nibabel.save(out_format(np.asarray(f.dataobj), affine, f.header), dst)
    if remove:
        for file in f.file_map.values():
            if os.path.exists(file.filename):
                logging.info(f'remove {os.path.basename(file.filename)}')
                os.remove(file.filename)


def make_affine(shape, voxel_size=1, orient='RAS', center='(x-1)/2'):
    """Generate an affine matrix with (0, 0, 0) in the center of the FOV

    Parameters
    ----------
    shape : list[int]
    voxel_size : list[float]
    orient : permutation of ['R' or 'L', 'A' or 'P', 'S' or 'I']
    center : list[float] or {"x/2", "x//2", "(x-1)/2", "(x-1)//2"}

    Returns
    -------
    affine : np.array
    """
    pos_orient = {'L': 'R', 'P': 'A', 'I': 'S'}
    pos_orient = [pos_orient.get(x, x) for x in orient]
    flip_orient = {'L': -1, 'P': -1, 'I': -1}
    flip_orient = [flip_orient.get(x, 1) for x in orient]
    perm = [pos_orient.index(x) for x in 'RAS']

    lin = np.eye(3)
    lin = lin * np.asarray(flip_orient) * np.asarray(voxel_size)
    lin = lin[perm, :]

    if isinstance(center, str):
        shape = np.asarray(shape)
        if center == '(x-1)/2':
            center = (shape - 1) / 2
        elif center == '(x-1)//2':
            center = (shape - 1) // 2
        elif center == 'x/2':
            center = shape / 2
        elif center == 'x//2':
            center = shape // 2
        else:
            raise ValueError('invalid value for `center`')
    else:
        center = np.asarray(center)

    aff = np.eye(4)
    aff[:3, :3] = lin
    aff[:3, -1:] = - lin @ center[:, None]

    return aff


def relabel(inp, lookup):
    """Relabel a label volume

    Parameters
    ----------
    inp : np.ndarray[integer]
        Input label volume
    lookup : dict[int, int or list[int]]
        Lookup table

    Returns
    -------
    out : np.ndarray[integer]
        Relabeled volume

    """
    out = np.zeros_like(inp)
    for dst, src in lookup.items():
        if hasattr(src, '__iter__'):
            for src1 in src:
                out[inp == src1] = dst
        else:
            out[inp == src] = dst
    return out


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


def write_json(src, dst, **kwargs):
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
    makedirs = kwargs.pop('makedirs', True)
    if isinstance(dst, (str, Path)):
        logging.info(f'write {os.path.basename(dst)}')
        if makedirs:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
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
        logging.info(f'write {os.path.basename(dst)}')
        if makedirs:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
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
        logging.info(f'write {os.path.basename(dst)}')
        if makedirs:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, 'wt', newline='') as fdst:
            return write_tsv(src, fdst, **kwargs, makedirs=False)
    kwargs.setdefault('delimiter', '\t')
    kwargs.setdefault('quoting', csv.QUOTE_NONE)
    writer = csv.writer(dst, **kwargs)
    writer.writerows(src)


def write_from_buffer(src, dst, makedirs=True):
    """
    Write from an open buffer

    Parameters
    ----------
    src : io.BufferedReader
        An object with the `read()` method
    dst : str or Path or file-like
        Output path

    Other Parameters
    ----------------
    makedirs : bool, default=True
        Create all directories needs to write the file
    """
    if isinstance(dst, (str, Path)):
        logging.info(f'write {os.path.basename(dst)}')
        if makedirs:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, 'wb') as fdst:
            return write_from_buffer(src, fdst, makedirs=False)
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
        logging.info(f'write {os.path.basename(dst)}')
        if makedirs:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
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
        logging.info(f'write {os.path.basename(dst)}')
        if makedirs:
            os.makedirs(os.path.dirname(dst), exist_ok=True)
        with open(dst, 'wb') as fdst:
            return copy_from_buffer(src, fdst, makedirs=False)
    if isinstance(src, (str, Path)):
        with open(src, 'rb') as fsrc:
            return copy_from_buffer(fsrc, dst, makedirs=False)
    dst.write(src.read())
