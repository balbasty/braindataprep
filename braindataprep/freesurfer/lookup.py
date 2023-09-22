
import os
import csv
from pathlib import Path


fs_lookup_path = os.path.join(
    os.path.dirname(__file__), 'lut', 'FreeSurferColorLUT.txt')
fs_dk_lookup_path = os.path.join(
    os.path.dirname(__file__), 'lut', 'colortable_desikan_killiany.txt')
fs_2005_lookup_path = os.path.join(
    os.path.dirname(__file__), 'lut', 'Simple_surface_labels2005.txt')
fs_2009_lookup_path = os.path.join(
    os.path.dirname(__file__), 'lut', 'Simple_surface_labels2009.txt')


def parse_fs_lookup(f, has_hemi=True):
    """Parse a freesurfer lookup table

    Parameters
    ----------
    path : str or file
        Freesurfer Lookup file.

        FS LUT are text files with the format "<INDEX> <NAME> <R> <G> <B> <A>"
        with any number of spaces separating the elements in each row. It is
        possible to have empty rows and comments (starting with "#").
        R/G/B/A are integer values between 0 and 255.

    has_hemi : bool
        Whether to look for "left"/"right"/"lh"/"rh"/etc in the index name
        and fill in a "hemisphere" column accordingly.

    Returns
    -------
    lut : list[list]
        The first row is the header
        `['index', 'name', ('hemisphere',) 'color']`
        All other rows contain the index (int), name (str),
        hemisphere ('bilateral', 'left' or 'right') and RGB color
        (as an hexadecimal string: '#000000') of a label.
        Note that the A (alpha) color channel gets dropped.
    """
    if isinstance(f, (str, Path)):
        with open(f, 'rb') as ff:
            return parse_fs_lookup(ff, has_hemi)

    if has_hemi:
        lookup = [['index', 'name', 'hemisphere', 'color']]
    else:
        lookup = [['index', 'name', 'color']]

    for line in f:
        line = line.decode().split('#')[0].strip()
        if not line:
            continue
        index, name, r, g, b, a = line.split()
        index, r, g, b, a = int(index), int(r), int(g), int(b), int(a)
        color = f'#{r:02x}{g:02x}{b:02x}'

        if not has_hemi:
            lookup.append([index, name, color])
        else:
            hemi = 'bilateral'
            if name.lower().startswith('left'):
                hemi = 'left'
                name = name[5:]
            if name.lower().startswith('right'):
                hemi = 'right'
                name = name[6:]
            if name.lower().startswith('l_'):
                hemi = 'left'
                name = name[2:]
            if name.lower().startswith('r_'):
                hemi = 'right'
                name = name[2:]
            if name.lower().startswith('lh.'):
                hemi = 'left'
                name = name[3:]
            if name.lower().startswith('rh.'):
                hemi = 'right'
                name = name[3:]
            if '-lh-' in name.lower():
                hemi = 'left'
                i = name.lower().find('-lh-')
                name = name[:i] + name[i+3:]
            if '-rh-' in name.lower():
                hemi = 'right'
                i = name.lower().find('-rh-')
                name = name[:i] + name[i+3:]
            if '_left_' in name.lower():
                hemi = 'left'
                i = name.lower().find('_left_')
                name = name[:i] + name[i+5:]
            if '_right_' in name.lower():
                hemi = 'right'
                i = name.lower().find('_right_')
                name = name[:i] + name[i+6:]
            lookup.append([index, name, hemi, color])

    return lookup


"""
A subset of FreeSurferColorLUT.txt indices that can be found in `"aseg.mgz"`
"""
aseg_labels = [
    0, 2, 3, 4, 5, 7, 8, 10, 11, 12, 13, 14, 15, 16, 17, 18, 24, 26, 28, 30,
    41, 42, 43, 44, 46, 47, 49, 50, 51, 52, 53, 54, 58, 60, 62, 63, 72, 77,
    78, 79, 80, 81, 82, 85, 251, 252, 253, 254, 255,
]

"""
A subset of FreeSurferColorLUT.txt indices that can be found in
`"aparc+aseg.mgz"`
"""
aparc_labels = [
    *range(1000, 1036),  # ctx-lh
    *range(2000, 2036),  # ctx-rh
    *range(3000, 3036),  # wm-lh
    *range(4000, 4036),  # wm-lh
]


def annot_to_lut(ctab, names):
    """Convert annotation metadata to a LUT

    `ctab` and `names` are returned by `nibabel.freesurfer.read_annot`.

    Parameters
    ----------
    ctab : ndarray[integer]
        Color table with shape (N, 5)
        The 5 columns correspond to [R, G, B, T, LabelIndex]
        where T = 255 - A
    names : list[bytes]
        Name corresponding to each ctab row

    Return
    ------
    lut : list[list]
        The first row is the header" `['index', 'name', 'color']`
        All other rows contain the index (int), name (str), and RGBA color
        (as an hexadecimal string: '#00000000') of a label
    """
    lut = [None] * (len(ctab) + 1)
    lut[0] = ['index', 'name', 'color']
    for n, (color, name) in enumerate(zip(ctab, names)):
        r, g, b, t, i = color.tolist()
        a = 255 - t
        color = f'#{r:02x}{g:02x}{b:02x}{a:02x}'
        lut[n] = [i, name, color]
    return lut


def filter_lookup(lut, labels):
    """Only include rows whose label is in `labels`"""
    return lut[:1] + [lkp for lkp in lut[1:] if lkp[0] in labels]


def write_lookup(path, mode=None):
    """
    Write a lookup table (LUT) as a tsv

    Parameters
    ----------
    path : str
        Output path
    mode : {'aseg', 'aparc+asaeg', 'dk', '2005', '2009'} or None or ndarray
        Which labeling scheme to use.
        - 'aseg' and 'aparc+aseg' are volumetric segmentations and include
          a "side" column (different labels for left and right)
        - 'dk', '2005' and '2009' and are surface segmentation and do not
          include a side column (since the segmentation of each hemisphere
          is already stored separately)
        - if `None`, store the full FS lookup table
        - else, it should contain a well formatted `list[list]`.
    """

    if mode == '2005':
        lookup = parse_fs_lookup(fs_2005_lookup_path, False)
    elif mode == '2009':
        lookup = parse_fs_lookup(fs_2009_lookup_path, False)
    elif mode == 'dk':
        lookup = parse_fs_lookup(fs_dk_lookup_path, False)
    elif isinstance(mode, str):
        lookup = parse_fs_lookup(fs_lookup_path, True)
        if mode == 'aseg':
            lookup = filter_lookup(lookup, aseg_labels)
        elif mode == 'aparc+aseg':
            lookup = filter_lookup(lookup, aseg_labels + aparc_labels)

    with open(path, 'wt', newline='') as f:
        writer = csv.writer(f, delimiter='\t', quoting=csv.QUOTE_NONE)
        for row in lookup:
            writer.writerow(row)
