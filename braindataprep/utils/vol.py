import numpy as np


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
