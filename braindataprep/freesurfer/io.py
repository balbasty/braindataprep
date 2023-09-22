import nibabel
import itertools
import numpy as np
import os


def closest_orientation(lin):
    """Find the closes orientation (RAS, LIA, etc) to an affine matrix"""
    ndim = len(lin)
    aff0 = np.eye(ndim, dtype=lin.dtype)
    best_aff = None
    best_sse = float('inf')
    for perm in itertools.permutations(range(ndim)):
        for flip in itertools.product([-1, 1], repeat=ndim):
            aff = aff0[:, perm]
            aff = aff * np.asarray(flip)
            sse = ((aff - lin)**2).sum()
            if sse < best_sse:
                best_aff = aff
                best_sse = sse
    return best_aff


def fs_surf2geom(meta):
    """
    Compute the geometry (affine and shape) of a FS surface

    Parameters
    ----------
    meta : dict
        Metadata dictionary returned by
        `nibabel.freesurfer.read_geometry(..., read_metadata=True)`

    Returns
    -------
    aff : ndarray
        vertex-to-RAS orientation matrix
    shape : ndarray
        shape of the original
    """
    shape = np.asarray(meta['volume'])
    vx = np.asarray(meta['voxelsize'])

    phys2ras = np.eye(4)
    x, y, z, c = meta['xras'], meta['yras'], meta['zras'], meta['cras']
    x, y, z, c = np.asarray(x), np.asarray(y), np.asarray(z), np.asarray(c)
    phys2ras[:-1, :] = np.stack([x, y, z, c], axis=1)

    orient2mesh = np.eye(4)
    orient2mesh[:-1, :-1] = closest_orientation(phys2ras[:-1, :-1])

    orient2phys = np.eye(4)
    orient2phys[np.arange(3), np.arange(3)] = vx

    aff = phys2ras @ orient2phys @ np.linalg.inv(orient2mesh)
    shape = shape.tolist()

    return aff, shape


# --------------------------------
# Converters that leverage nibabel
# --------------------------------


def nibabel_fs2gii(src, dst, remove=False):
    if dst.endswith('.surf.gii'):
        return nibabel_fs2gii_surf(src, dst, remove)
    if dst.endswith('.shape.gii'):
        return nibabel_fs2gii_shape(src, dst, remove)
    if dst.endswith('.label.gii'):
        return nibabel_fs2gii_shape(src, dst, remove)
    raise NotImplementedError('converter for output', os.path.basename(dst))


def nibabel_fs2gii_surf(src, dst, remove=False):
    """
    Convert a surface from FS to gifti
    """
    v, f, meta = nibabel.freesurfer.read_geometry(src, read_metadata=True)

    coord = nibabel.gifti.GiftiCoordSystem(xform=fs_surf2geom(meta)[0])

    gii = nibabel.GiftiImage()
    gii.add_gifti_data_array(nibabel.gifti.GiftiDataArray(
        v, intent='pointset', coordsys=coord, datatype='float32',
    ))
    gii.add_gifti_data_array(nibabel.gifti.GiftiDataArray(
        f, intent='triangle', datatype='int32',
    ))
    nibabel.save(gii, dst)
    if remove:
        os.remove(src)


def nibabel_fs2gii_shape(src, dst, remove=False):
    """
    Convert a surface shape from FS to gifti
    """
    x = nibabel.freesurfer.read_morph_data(src)

    gii = nibabel.GiftiImage()
    gii.add_gifti_data_array(nibabel.gifti.GiftiDataArray(
        x, intent='shape', datatype='float32',
    ))
    nibabel.save(gii, dst)
    if remove:
        os.remove(src)


def nibabel_fs2gii_label(src, dst, remove=False):
    """
    Convert a surface label from FS to gifti
    """
    labels, colors, names = nibabel.freesurfer.read_annot(src)

    table = nibabel.gifti.GiftiLabelTable()
    for k, c in enumerate(colors):
        table.labels.append(nibabel.gifti.GiftiLabel(k, c[0], c[1], c[2]))

    gii = nibabel.GiftiImage(labeltable=table)
    gii.add_gifti_data_array(nibabel.gifti.GiftiDataArray(
        labels, intent='label', datatype='int32',
    ))
    nibabel.save(gii, dst)
    if remove:
        os.remove(src)


# ---------------------------------------------------------------
# Low-level readers/converters for formats not handled by nibabel
# ---------------------------------------------------------------


TAG_OLD_COLORTABLE = 1
TAG_OLD_USEREALRAS = 2
TAG_CMDLINE = 3
TAG_USEREALRAS = 4
TAG_COLORTABLE = 5

TAG_GCAMORPH_GEOM = 10
TAG_GCAMORPH_TYPE = 11
TAG_GCAMORPH_LABELS = 12
TAG_GCAMORPH_META = 13
TAG_GCAMORPH_AFFINE = 14

TAG_OLD_SURF_GEOM = 20
TAG_SURF_GEOM = 21

TAG_OLD_MGH_XFORM = 30
TAG_MGH_XFORM = 31
TAG_GROUP_AVG_SURFACE_AREA = 32

TAG_AUTO_ALIGN = 33

TAG_SCALAR_DOUBLE = 40
TAG_PEDIR = 41
TAG_MRI_FRAME = 42
TAG_FIELDSTRENGTH = 43
TAG_ORIG_RAS2VOX = 44

GCSA_MAGIC = 0xababcdcd
GIBBS_SURFACE_NEIGHBORS = 4

MATRIX_REAL = 1
MATRIX_COMPLEX = 2


def read_gcs(path):
    """Read a Gaussian Classifier Surface Atlas (GCSA) (`"*.gcs"`)

    Parameters
    ----------
    path :src

    Returns
    -------
    inputs : list[dict]
        Each element has fields:
        ```
        {
            'type': int,
            'fname': str,
            'navgs': int,
            'flags': int,
        }
        ```
    classifier : dict
        The classifier has fields:
        ```
        {
            'icno': int,    # icosphere order of the classifier
            'vertices': [
                {
                    'total_training': int,
                    'labels': [
                        {
                            'label': int,
                            'total_training': int,
                            'v_means': np.ndarray,
                            'm_cov': np.ndarray,
                        },
                        ...  # * nlabels
                    ]
                },
                ... # * nvertices
            ]
        }
        ```
    prior : dict
        The prior has fields:
        ```
        {
            'icno': int,    # icosphere order of the classifier
            'vertices': [
                {
                    'total_training': int,
                    'labels': [
                        {
                            'label': int,
                            'prior': float,
                            'neighbors': [
                                {
                                    'total_nbrs': int,
                                    'labels': [
                                        {
                                            'label': int,
                                            'prior': float,
                                        },
                                        ... # * nlabels
                                    ]
                                },
                                ... # * nneighbors
                            ],
                        },
                        ...  # * nlabels
                    ]
                },
                ... # * nvertices
            ]
        }
        ```
    ctab : list[(str, str)]
        Color table. May be `None`.
    """
    def icno_to_nvert(icno):
        # a regular icosahedron has 12 vertices, 30 edges, 20 faces
        # at each refinement level, one vertex is added to each edge,
        # each edge therfore gives rise to 2 new edges, and each face
        # gives rise to 3 additional edges (and 3 faces)
        def refine(v, e, f):
            v = v + e
            e = 2*e + 3*f
            f = 4*f
            return v, e, f
        v, e, f = 12, 30, 20
        for _ in range(icno):
            v, e, f = refine(v, e, f)
        return v, e, f

    with open(path, 'rb') as f:
        # read magic number
        magic = readitem(f, '>u4')
        if magic == GCSA_MAGIC:
            ENDIAN = '>'
        elif magic.byteswap() == GCSA_MAGIC:
            ENDIAN = '<'
        else:
            raise ValueError(
                f'Is this a gcs file? Magic number does not match: '
                f'{magic:08x} != {GCSA_MAGIC:08x}')

        # read header
        ninputs, icno_classifiers, icno_priors = readitems(f, 3, f'{ENDIAN}i4')

        # parse inputs
        inputs = [None] * ninputs
        for n in range(ninputs):
            print(f'read inputs | {n+1}/{ninputs}', end='\r')
            input_type, fname_size = readitems(f, 2, f'{ENDIAN}i4')
            fname = f.read(fname_size).decode()[:-1]
            navgs, flags = readitems(f, 2, f'{ENDIAN}i4')
            inputs[n] = {
                'type': input_type,
                'fname': fname,
                'navgs': navgs,
                'flags': flags,
            }
        print('')

        # parse classifier
        nc = icno_to_nvert(icno_classifiers)[0]
        print('read classifier | icno:', icno_classifiers, '| vertices:', nc)
        classifier = [None] * nc
        for n in range(nc):
            print(f'read classifier | {n+1}/{nc}', end='\r')
            nlabels, total_training = readitems(f, 2, f'{ENDIAN}i4')
            labels = [None] * nlabels
            for m in range(nlabels):
                label, label_total_training = readitems(f, 2, f'{ENDIAN}i4')
                v_means = read_matrix_ascii(f)
                m_cov = read_matrix_ascii(f)
                labels[m] = {
                    'label': label,
                    'total_training': label_total_training,
                    'v_means': v_means,
                    'm_cov': m_cov,
                }
            classifier[n] = {
                'total_training': total_training,
                'labels': labels,
            }
        classifier = {
            'icno': icno_classifiers,
            'vertices': classifier,
        }
        print('')

        # parse prior
        nc = icno_to_nvert(icno_priors)[0]
        print('read prior | icno:', icno_priors, '| vertices:', nc)
        prior = [None] * nc
        for n in range(nc):
            print(f'read prior | {n+1}/{nc}', end='\r')
            nilabels, total_training = readitems(f, 2, f'{ENDIAN}i4')
            ilabels = [None] * nilabels
            for i in range(nilabels):
                ilabel = readitem(f, f'{ENDIAN}i4')
                iprior = readitem(f, f'{ENDIAN}f4')
                neighbors = [None] * GIBBS_SURFACE_NEIGHBORS
                for k in range(GIBBS_SURFACE_NEIGHBORS):
                    total_nbrs, njlabels = readitems(f, 2, f'{ENDIAN}i4')
                    jlabels = [None] * njlabels
                    for j in range(njlabels):
                        jlabel = readitem(f, f'{ENDIAN}i4')
                        jprior = readitem(f, f'{ENDIAN}f4')
                        jlabels[j] = {
                            'label': jlabel,
                            'prior': jprior,
                        }
                    neighbors[k] = {
                        'total_nbrs': total_nbrs,
                        'labels': jlabels,
                    }
                ilabels[i] = {
                    'label': ilabel,
                    'prior': iprior,
                    'neighbors': neighbors,
                }
            prior[n] = {
                'total_training': total_training,
                'labels': ilabels,
            }
        prior = {
            'icno': icno_priors,
            'vertices': prior,
        }
        print('')

        # parse color table
        if f:
            tag = readitem(f, dtype=f'{ENDIAN}i4')
            if tag == TAG_OLD_COLORTABLE:
                ctab, *_ = read_ctab_binary(f, ENDIAN)

    return inputs, classifier, prior, ctab


def read_ctab_binary(f, ENDIAN='>'):
    """Read a binary color table

    Parameters
    ----------
    f : str or file
        Path to a file, or open file object
    ENDIAN : {'<', '>'}
        Endianness

    Returns
    -------
    ctab : list[(str, str))]
        Each element in the list contains the region name and the
        hexadecimal representation of a  color (e.g. '#000000')
    fname : str
        Stored filename
    version : {1, 2}
        Colortab format version
    """
    if isinstance(f, str):
        with open(f, 'rb') as ff:
            return read_ctab_binary(ff)

    version = readitem(f, f'{ENDIAN}i4')

    def read_v1(f, nentries):
        fname_size = np.frombuffer(f.read(4), dtype=f'{ENDIAN}i4').item()
        fname = f.read(fname_size).decode()[:-1]
        ctab = [None] * nentries
        for n in range(nentries):
            print(f'read ctab | {n+1}/{nentries}', end='\r')
            name_size = readitem(f, f'{ENDIAN}i4')
            name = f.read(name_size).decode()[:-1]
            r, g, b, a = readitems(f, 4, f'{ENDIAN}i4')
            rgba = f'#{r:02x}{g:02x}{b:02x}{255 - a:02x}'
            rgba = f'#{r:02x}{g:02x}{b:02x}{a:02x}'
            ctab[n] = [name, rgba]
        print('')
        return ctab, fname

    def read_v2(f):
        max_nentries, fname_size = readitems(f, 2, f'{ENDIAN}i4')
        fname = f.read(fname_size).decode()[:-1]
        nentries = readitem(f, f'{ENDIAN}i4')
        ctab = [None] * max_nentries
        for n in range(nentries):
            print(f'read ctab | {n+1}/{nentries}', end='\r')
            structure, name_size = readitems(f, 2, f'{ENDIAN}i4')
            name = f.read(name_size).decode()[:-1]
            r, g, b, a = readitems(f, 4, f'{ENDIAN}i4')
            rgba = f'#{r:02x}{g:02x}{b:02x}{255 - a:02x}'
            ctab[structure] = [name, rgba]
        print('')
        return ctab, fname

    if version > 0:
        print('version 1')
        return *read_v1(f, version), 1
    else:
        version = -version
        if version == 2:
            print('version 2')
            return *read_v2(f), 2
        else:
            raise ValueError('Bad version', version)


def read_matrix_ascii(f):
    """Read an ASCII matrix representation (single file or within a file)

    Parameters
    ----------
    f : str or file
        Path to a file, or open file object

    Returns
    -------
    mat : np.ndarray[float64 or complex128]
        Matrix
    """
    if isinstance(f, str):
        with open(f, 'rb') as ff:
            return read_matrix_ascii(ff)

    # parse header
    line = f.readline()
    type, rows, cols = map(int, line.decode().split())

    # parse matrix
    dtype = 'float64' if type == MATRIX_REAL else 'complex128'
    mat = np.empty([rows, cols], dtype=dtype)
    for n in range(rows):
        line = list(map(float, f.readline().decode().split()))
        if type == MATRIX_COMPLEX:
            real, imag = line[::2], line[1::2]
            line = [r + 1j*i for r, i in zip(real, imag)]
        mat[n] = line

    return mat


def read_ico(f):
    """Read files 'ico{d}.tri'

    Parameters
    ----------
    f : str or file
        Path to a file, or open file object

    Returns
    -------
    vertices : np.ndarray[double]
        Array of vertices with shape (N, 3)
    faces : np.ndarray[long]
        Array of triangles with shape (M, 3)
    """
    if isinstance(f, str):
        with open(f, 'rb') as ff:
            return read_ico(ff)
    nb_vertices = int(f.readline().strip())
    vertices = np.empty([nb_vertices, 3], dtype='float64')
    for n in range(nb_vertices):
        vertices[n] = list(map(float, f.readline().split()))
    nb_faces = int(f.readline().strip())
    faces = np.empty([nb_faces, 3], dtype='int64')
    for n in range(nb_faces):
        faces[n] = list(map(int, f.readline().split()))
    return vertices, faces


def readitem(f, dtype):
    """
    Read one item of the given data type,
    and return as a python scalar
    """
    dtype = np.dtype(dtype)
    return np.frombuffer(
        f.read(dtype.itemsize), dtype=dtype
    ).item()


def readitems(f, n, dtype):
    """
    Read `n` items of the given data type,
    and return as a list of python scalar
    """
    dtype = np.dtype(dtype)
    return np.frombuffer(
        f.read(dtype.itemsize * n), dtype=dtype, count=n
    ).tolist()
