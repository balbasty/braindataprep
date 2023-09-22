import urllib.request
import os
import time
import nibabel
import json
from pathlib import Path
import numpy as np


def download_file(url, path=None, packet_size=1024):
    """
    Download a file

    Parameters
    ----------
    url : str
        File URL.
    path : str
        Output path.
    packet_size : int
        Download packets of this size.
        If None, download the entire file at once.

    Returns
    -------
    path : str
        Output path.
    """
    if path is None:
        path = os.path.join('.', os.path.basename(url))
    if os.path.isdir(path):
        path = os.path.join(path, os.path.basename(url))
    os.makedirs(os.path.dirname(path), exist_ok=True)

    with urllib.request.urlopen(url) as finp:
        total_size = finp.getheader("Content-Length")
        if total_size:
            total_size = int(total_size)
        else:
            total_size = None
        with open(path, 'wb') as fout:
            if packet_size:
                packet_sum = 0
                print('')
                while finp:
                    tic = time.time()
                    packet = finp.read(packet_size)
                    tac = time.time()
                    if not packet:
                        break
                    fout.write(packet)
                    toc = time.time()
                    packet_sum += packet_size
                    show_download_progress(
                        packet_sum, total_size,
                        time=(packet_size, tic, tac, toc)
                    )
                if total_size and (packet_sum != total_size):
                    print('  INCOMPLETE')
                else:
                    print('  COMPLETE')
            else:
                fout.write(finp.read())
    return path


def show_download_progress(size, total_size=None, time=None, end='\r'):
    size, size_unit = round_bytes(size)
    print(f'{end}{size:7.3f} {size_unit}', end='')
    if total_size:
        total_size, total_unit = round_bytes(total_size)
        print(f' / {total_size:7.3f} {total_unit}', end='')
    if time:
        packet_size, tic, tac, toc = time
        tb, tb_unit = round_bytes(packet_size / (toc - tic))
        db, db_unit = round_bytes(packet_size / (tac - tic))
        wb, wb_unit = round_bytes(packet_size / (toc - tac))
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


def nibabel_convert(src, dst, remove=False,
                    inp_format=None, out_format=None, affine=None):
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
    nibabel.save(out_format(np.asarray(f.dataobj), affine, f.header), dst)
    if remove:
        for file in f.file_map.values():
            if os.path.exists(file.filename):
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


def write_json(obj, f, **kwargs):
    if isinstance(f, (str, Path)):
        with open(f, 'wt') as ff:
            return write_json(obj, ff, **kwargs)
    kwargs.setdefault('indent', 2)
    json.dump(obj, f, **kwargs)
