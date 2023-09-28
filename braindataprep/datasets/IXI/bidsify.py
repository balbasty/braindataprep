import os
import sys
import click
import tarfile
import logging
import nibabel
import glob
import numpy as np
from braindataprep.utils import (
    get_tree_path,
    fileparts,
    copy_from_buffer,
    copy_json,
    write_tsv,
)


try:
    import xlrd
except ImportError:
    logging.error(
        'Cannot find `xlrd`. Did you install with [ixi] flag? '
        'Try `pip install braindataprep[ixi]`.'
    )

"""
Expected input
--------------
IXI/
  sourcedata/
      IXI-T1.tar
      IXI-T2.tar
      IXI-PD.tar
      IXI-MRA.tar
      IXI-DTI.tar
      bvecs.txt
      bvals.txt
      IXI.xls

Expected output
---------------
IXI/
  dataset_description.json
  participants.tsv
  participants.json
  rawdata/
      dwi.bval
      dwi.bvec
      sub-{03d}/
          anat/
              sub-{03d}_T1w.nii.gz
              sub-{03d}_T1w.json
              sub-{03d}_T2w.nii.gz
              sub-{03d}_T2w.json
              sub-{03d}_PDw.nii.gz
              sub-{03d}_PDw.json
              sub-{03d}_angio.nii.gz
              sub-{03d}_angio.json
          dwi/
              sub-{03d}_dwi.nii.gz
              sub-{03d}_dwi.json
"""

"""Folder containing template README/JSON/..."""
TPLDIR = os.path.join(os.path.dirname(__file__), 'templates')

modality_ixi2bids = {
    'T1': 'T1w',
    'T2': 'T2w',
    'PD': 'PDw',
    'MRA': 'angio',
    'DTI': 'dwi',
}
modality_bids2ixi = {
    'T1w': 'T1',
    'T2w': 'T2',
    'PDw': 'PD',
    'angio': 'MRA',
    'dwi': 'DTI',
}


@click.command()
@click.option(
    '--path', default=None,
    help='Path to tree')
@click.option(
    '--key', multiple=True,
    type=click.Choice(["meta", "json", "T1w", "T2w", "PDw", "angio", "dwi"]),
    help='Only bidsify these keys')
@click.option(
    '--sub', multiple=True, type=int,
    help='Only download these subjects')
@click.option(
    '--json-only', is_flag=True, default=False,
    help='Only write jsons (not volumes)'
)
def bidsify(path, key, sub, json_only):
    logging.info('IXI - bidsify')
    path = get_tree_path(path)
    keys = set(key or ['meta', 'json', 'T1w', 'T2w', 'PDw', 'angio', 'dwi'])
    subs = sub
    ixipath = os.path.join(path, 'IXI')
    src = os.path.join(ixipath, 'sourcedata')
    raw = os.path.join(ixipath, 'rawdata')

    if 'meta' in keys:
        # The mapping from subject to site is only available through
        # individual filenames. We therefore need to first parse one of
        # the tar to build this mapping.
        sites = None
        for key in ['T1', 'T2', 'PD', 'MRA', 'DTI']:
            tarpath = os.path.join(src, f'IXI-{key}.tar')
            if os.path.exists(tarpath):
                sites = get_sites(tarpath)
                break
        if sites is None:
            logging.error("No tar file available. Cannot compute sites.")

        copy_from_buffer(
            os.path.join(TPLDIR, 'README'),
            os.path.join(ixipath, 'README'),
        )

        copy_json(
            os.path.join(TPLDIR, 'dataset_description.json'),
            os.path.join(ixipath, 'dataset_description.json')
        )

        copy_json(
            os.path.join(TPLDIR, 'participants.json'),
            os.path.join(ixipath, 'participants.json')
        )

        make_participants(
            os.path.join(src, 'IXI.xls'),
            os.path.join(ixipath, 'participants.tsv'),
            sites,
        )

    # T1/T2/PD/MRA are simple "anat" scans that can be processed
    # identically.
    for key in keys.intersection(set(['T1w', 'T2w', 'PDw', 'angio'])):
        # check that the archive is available
        tarpath = os.path.join(src, f'IXI-{modality_bids2ixi[key]}.tar')
        if not os.path.exists(tarpath):
            logging.warning(f'IXI-{modality_bids2ixi[key]}.tar not found')
            continue
        # parse the archive
        with tarfile.open(tarpath) as f:
            for member in f.getmembers():
                id, site, *_ = member.name.split('-')
                id = int(id[3:])
                if subs and id not in subs:
                    continue
                dst = os.path.join(raw, f'sub-{id:03d}', 'anat')
                if 'json' in keys:
                    copy_json(
                        os.path.join(TPLDIR, site, f'{key}.json'),
                        os.path.join(dst, f'sub-{id:03d}_{key}.json'),
                    )
                if not json_only:
                    copy_from_buffer(
                        f.extractfile(member),
                        os.path.join(dst, f'sub-{id:03d}_{key}.nii.gz'),
                    )

    # DWI scans are stored as individual 3D niftis (one per bval/bvec)
    # whereas BIDS prefers 4D niftis (actually 5D, since nifti specifies
    # that the 4th dimension is reserved for time)
    # We also need to deal with the bvals/bvecs files.
    if 'dwi' in keys:
        tarpath = os.path.join(src, 'IXI-DTI.tar')
        if not os.path.exists(tarpath):
            logging.warning('IXI-DTI.tar not found')
            return

        # First, copy bvals/bvecs.
        # They are common to all subjects so we place them at the
        # top of the tree (under "rawdata/")
        if not os.path.exists(os.path.join(src, 'bvals.txt')):
            logging.error('bvals not found')
            return
        if not os.path.exists(os.path.join(src, 'bvecs.txt')):
            logging.error('bvecs not found')
            return
        copy_from_buffer(
            os.path.join(src, 'bvals.txt'),
            os.path.join(raw, 'dwi.bval')
        )
        copy_from_buffer(
            os.path.join(src, 'bvecs.txt'),
            os.path.join(raw, 'dwi.bvec')
        )
        # Then extract individual 3D volumes and save them with
        # temporary names (we use the non-BIDS-compliant
        # "ch-{index}" tag)
        with tarfile.open(tarpath) as f:
            ids = {}
            for member in f.getmembers():
                _, basename, _ = fileparts(member.name)
                id, site, *_, dti_id = basename.split('-')
                id = int(id[3:])
                if subs and id not in subs:
                    continue
                ids[id] = site
                dti_id = int(dti_id)
                dst = os.path.join(raw, f'sub-{id:03d}', 'dwi')
                basename = f'sub-{id:03d}_ch-{dti_id:02d}_dwi.nii.gz'
                if not json_only:
                    copy_from_buffer(
                        f.extractfile(member),
                        os.path.join(dst, basename)
                    )
        # Finally, go through each subject and combine the
        # 3D b-series into 5D niftis
        for id, site in ids.items():
            # if we only write JSON, do it separately
            dst = os.path.join(raw, f'sub-{id:03d}', 'dwi')
            if json_only:
                copy_json(
                    os.path.join(TPLDIR, site, 'dwi.json'),
                    os.path.join(dst, f'sub-{id:03d}_dwi.json')
                )
                continue
            # now, concatenate volumes
            logging.info(f'write sub-{id:03d}_dwi.nii.gz')
            fnames = list(sorted(glob.glob(
                os.path.join(dst, f'sub-{id:03d}_ch-*_dwi.nii.gz')
            )))
            if not fnames:
                continue
            mapped_vol = nibabel.load(fnames[0])
            affine, header = mapped_vol.affine, mapped_vol.header
            dat = [np.asarray(mapped_vol.dataobj).squeeze()]
            for fname in fnames:
                dat += [np.asarray(nibabel.load(fname).dataobj).squeeze()]
            if len(set([tuple(dat1.shape) for dat1 in dat])) > 1:
                # for some reason, this happened in one of the subjects...
                logging.error('sub-{id:03d}_dwi | shapes not compatible')
                for fname in fnames:
                    logging.info(f'remove {os.path.basename(fname)}')
                    os.remove(fname)
                continue
            # ensure 5D
            dat = list(map(lambda x: x[..., None, None], dat))
            # concatenate along the 5-th dimension (indexed 4)
            # as the 4-th dimension is reserved for time
            dat = np.concatenate(dat, axis=4)
            nibabel.save(
                nibabel.Nifti1Image(dat, affine, header),
                os.path.join(dst, f'sub-{id:03d}_dwi.nii.gz')
            )
            if 'json' in keys:
                copy_json(
                    os.path.join(TPLDIR, site, 'dwi.json'),
                    os.path.join(dst, f'sub-{id:03d}_dwi.json')
                )
            for fname in fnames:
                logging.info(f'remove {os.path.basename(fname)}')
                os.remove(fname)


def get_sites(path_tar):
    sitemap = {}
    with tarfile.open(path_tar) as f:
        for member in f.getmembers():
            ixi_id, site, *_ = member.name.split('-')
            ixi_id = int(ixi_id[3:])
            sitemap[ixi_id] = site
    return sitemap


def make_participants(path_xls, path_tsv, sites):
    book = xlrd.open_workbook(path_xls)
    sheet = book.sheet_by_index(0)

    ixi_header = [
        'IXI_ID',
        'SEX_ID',
        'HEIGHT',
        'WEIGHT',
        'ETHNIC_ID',
        'MARITAL_ID',
        'OCCUPATION_ID',
        'QUALIFICATION_ID',
        'DOB',
        'DATE_AVAILABLE',
        'STUDY_DATE',
        'AGE',
    ]
    ixi_age = {
        1: 'M',     # Male
        2: 'F',     # Female
    }
    ixi_ethnicity = {
        1: 'W',     # White
        2: 'B',     # Black or black british
        3: 'A',     # Asian or asian british
        5: 'C',     # Chinese
        6: 'O',     # Other
    }
    ixi_marital_status = {
        1: 'S',     # Single
        2: 'M',     # Married
        3: 'C',     # Cohabiting
        4: 'D',     # Divorced/separated
        5: 'W',     # Widowed
    }
    ixi_occupation = {
        1: 'FT',    # Go out to full time employment
        2: 'PT',    # Go out to part time employment (<25hrs)
        3: 'S',     # Study at college or university
        4: 'H',     # Full-time housework
        5: 'R',     # Retired
        6: 'U',     # Unemployed
        7: 'WFH',   # Work for pay at home
        8: 'O',     # Other
    }
    ixi_qualification = {
        1: 'N',     # No qualifications
        2: 'O',     # O-levels, GCSEs, or CSEs
        3: 'A',     # A-levels
        4: 'F',     # Further education e.g. City & Guilds / NVQs
        5: 'U',     # University or Polytechnic degree
    }
    participants_header = [
        'participant_id',
        'site',
        'age',
        'sex',
        'height',
        'weight',
        'dob',
        'ethnicity',
        'marital_status',
        'occupation',
        'qualification',
        'study_date',
    ]

    def iter_rows():
        yield participants_header
        for n in range(1, sheet.nrows):
            ixi_row = sheet.row(n)
            if ixi_row[9].value == 0:
                continue
            ixi_id = int(ixi_row[ixi_header.index("IXI_ID")].value)
            if ixi_id not in sites:
                continue
            participant = [
                f'sub-{ixi_id:03d}',
                sites[ixi_id],
                ixi_row[ixi_header.index('AGE')].value,
                ixi_age.get(
                    ixi_row[ixi_header.index('SEX_ID')].value,
                    'n/a'),
                ixi_row[ixi_header.index('HEIGHT')].value,
                ixi_row[ixi_header.index('WEIGHT')].value,
                ixi_row[ixi_header.index('DOB')].value,
                ixi_ethnicity.get(
                    ixi_row[ixi_header.index('ETHNIC_ID')].value,
                    'n/a'),
                ixi_marital_status.get(
                    ixi_row[ixi_header.index('MARITAL_ID')].value,
                    'n/a'),
                ixi_occupation.get(
                    ixi_row[ixi_header.index('OCCUPATION_ID')].value,
                    'n/a'),
                ixi_qualification.get(
                    ixi_row[ixi_header.index('QUALIFICATION_ID')].value,
                    'n/a'),
                ixi_row[ixi_header.index('STUDY_DATE')].value,
            ]
            yield participant

    write_tsv(iter_rows(), path_tsv)


if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(0)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(0)
    formatter = logging.Formatter('%(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    bidsify()
