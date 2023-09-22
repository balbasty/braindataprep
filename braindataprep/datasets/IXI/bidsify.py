import os
import sys
import click
import tarfile
import logging
import csv
import json
import nibabel
import glob
import shutil
import numpy as np
from braindataprep.utils import fileparts

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

readme = """
# IXI Dataset

## Demographics

```
+================+====================================================================+
| site           | The data has been collected at three different hospitals in London |
+================+====================================================================+
| Guys           | Guy's Hospital, Great Maze Pond, London SE1 9RT, UK                |
+----------------+--------------------------------------------------------------------+
| HH             | Hammersmith Hospital, 72 Du Cane Rd, London W12 0HS, UK            |
+----------------+--------------------------------------------------------------------+
| IOP            | Institute of Psychiatry, Denmark Hill, London SE5 8AB, UK          |
+================+====================================================================+
| sex            | Sex of the participant as reported by the participant              |
+================+====================================================================+
| M              | male                                                               |
+----------------+--------------------------------------------------------------------+
| F              | female                                                             |
+================+====================================================================+
| ethnicity      | Ethnicity of the participant as reported by the participant        |
+================+====================================================================+
| W              | white                                                              |
+----------------+--------------------------------------------------------------------+
| B              | black or black british                                             |
+----------------+--------------------------------------------------------------------+
| A              | asian or asian british                                             |
+----------------+--------------------------------------------------------------------+
| C              | chinese                                                            |
+----------------+--------------------------------------------------------------------+
| O              | other                                                              |
+================+====================================================================+
| marital_status | Marital status of the participant                                  |
+================+====================================================================+
| S              | single                                                             |
+----------------+--------------------------------------------------------------------+
| M              | married                                                            |
+----------------+--------------------------------------------------------------------+
| C              | cohabiting                                                         |
+----------------+--------------------------------------------------------------------+
| D              | divorced/separated                                                 |
+----------------+--------------------------------------------------------------------+
| W              | widowed                                                            |
+================+====================================================================+
| occupation     | Occupation of the participant as reported by the participant       |
+================+====================================================================+
| FT             | go out to full time employment                                     |
+----------------+--------------------------------------------------------------------+
| PT             | go out to part time employment (<25hrs)                            |
+----------------+--------------------------------------------------------------------+
| S              | study at college or university                                     |
+----------------+--------------------------------------------------------------------+
| H              | full-time housework                                                |
+----------------+--------------------------------------------------------------------+
| R              | retired                                                            |
+----------------+--------------------------------------------------------------------+
| U              | unemployed                                                         |
+----------------+--------------------------------------------------------------------+
| WFH            | work for pay at home                                               |
+----------------+--------------------------------------------------------------------+
| O              | other                                                              |
+================+====================================================================+
| qualification  | Qualification of the participant as reported by the participant    |
+================+====================================================================+
| N              | No qualifications                                                  |
+----------------+--------------------------------------------------------------------+
| O              | O-levels, GCSEs, or CSEs                                           |
+----------------+--------------------------------------------------------------------+
| A              | A-levels                                                           |
+----------------+--------------------------------------------------------------------+
| F              | Further education e.g. City & Guilds / NVQs                        |
+----------------+--------------------------------------------------------------------+
| U              | University or Polytechnic degree                                   |
+----------------+--------------------------------------------------------------------+
```

## Notes from the IXI website

In this project we have collected nearly 600 MR images from normal,
healthy subjects. The MR image acquisition protocol for each subject includes:

- T1, T2 and PD-weighted images
- MRA images
- Diffusion-weighted images (15 directions)

The data has been collected at three different hospitals in London:

- Hammersmith Hospital using a Philips 3T system
  ([details of scanner parameters](http://brain-development.org/scanner-philips-medical-systems-intera-3t/))
- Guy's Hospital using a Philips 1.5T system
  ([details of scanner parameters](http://brain-development.org/scanner-philips-medical-systems-gyroscan-intera-1-5t/))
- Institute of Psychiatry using a GE 1.5T system (details of the scan parameters not available at the moment)

The data has been collected as part of the project:

**IXI - Information eXtraction from Images (EPSRC GR/S21533/02)**

The images in NIFTI format can be downloaded from here:

- T1 images ([all images](http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI/IXI-T1.tar))
- T2 images ([all images](http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI/IXI-T2.tar))
- PD images ([all images](http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI/IXI-PD.tar))
- MRA images ([all images](http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI/IXI-MRA.tar))
- DTI images ([all images](http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI/IXI-DTI.tar),
  [bvecs.txt](http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI/bvecs.txt),
  [bvals.txt](http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI/bvals.txt))
- Demographic information ([spreadsheet](http://biomedic.doc.ic.ac.uk/brain-development/downloads/IXI/IXI.xls))

This data is made available under the Creative Commons
[CC BY-SA 3.0 license](https://creativecommons.org/licenses/by-sa/3.0/legalcode).
If you use the IXI data please acknowledge the source of the IXI data,
e.g. this website.

"""  # noqa: E501

"""
JSONs
"""
dataset_description_json = {
    'Name': 'IXI',
    'BIDSVersion': '1.8.0',
    'LICENSE': 'CC BY-SA 3.0',
    'Acknowledgements': 'http://brain-development.org/ixi-dataset/',
}

participants_json = {
    "site": {
        "Description": "study site",
        "Levels": {
            "Guys": "Guy's Hospital, Great Maze Pond, London SE1 9RT, UK",
            "HH": "Hammersmith Hospital, 72 Du Cane Rd, London W12 0HS, UK",
            "IOP": "Institute of Psychiatry, Denmark Hill, London SE5 8AB, UK",
        }
    },
    "age": {
        "Description": "age of the participant",
        "Units": "years"
    },
    "sex": {
        "Description": "sex of the participant as reported by the participant",
        "Levels": {
            "M": "male",
            "F": "female"
        }
    },
    "height": {
        "Description": "height of the participant",
        "Units": "meters"
    },
    "weight": {
        "Description": "weight of the participant",
        "Units": "kilograms"
    },
    "dob": {
        "Description": "date of birth of the participant",
        "Units": "yyyy-mm-dd"
    },
    "ethnicity": {
        "Description": "ethnicity of the participant",
        "Levels": {
            "W": "white",
            "B": "black or black british",
            "A": "asian or asian british",
            "C": "chinese",
            "O": "other"
        }
    },
    "marital_status": {
        "Description": "marital status of the participant",
        "Levels": {
            "S": "single",
            "M": "married",
            "C": "cohabiting",
            "D": "divorced/separated",
            "W": "widowed"
        }
    },
    "occupation": {
        "Description": "occupation of the participant",
        "Levels": {
            "FT": "go out to full time employment",
            "PT": "go out to part time employment (<25hrs)",
            "S": "study at college or university",
            "H": "full-time housework",
            "R": "retired",
            "U": "unemployed",
            "WFH": "work for pay at home",
            "O": "other"
        }
    },
    "qualification": {
        "Description": "qualification of the participant",
        "Levels": {
            "N": "No qualifications",
            "O": "O-levels, GCSEs, or CSEs",
            "A": "A-levels",
            "F": "Further education e.g. City & Guilds / NVQs",
            "U": "University or Polytechnic degree"
        }
    },
    "study_date": {
        "Description": "date the study was performed",
        "Units": "yyyy-mm-dd"
    },
}


def scans_json(site):
    return {
        'Manufacturer': 'GE' if site == 'IOP' else 'Philips',
        'MagneticFieldStrength': 3 if site == 'HH' else 1.5,
    }


modality_json = {
    'T1': lambda site: {
            'PulseSequenceType': 'Gradient Echo',
            'EchoTime': (
                4.60269975662231E-3 if site == 'HH' else
                4.603E-3 if site == 'Guys' else
                None),
            'RepetitionTimeExcitation': (
                9.60000038146972E-3 if site == 'HH' else
                9.813E-3 if site == 'Guys' else
                None),
            'FlipAngle': 8 if site in ('HH', 'Guys') else None,
        },
    'T2': lambda site: {
            'PulseSequenceType': 'Fast Spin Echo',
            'EchoTime': 8E-3 if site in ('HH', 'Guys') else None,
            'RepetitionTimeExcitation': (
                5725.79052734375E-3 if site == 'HH' else
                8178.34E-3 if site == 'Guys' else
                None),
            'FlipAngle': 90 if site in ('HH', 'Guys') else None,
        },
    'PD': lambda site: {
            'PulseSequenceType': 'Fast Spin Echo',
            'EchoTime': 100E-3 if site in ('HH', 'Guys') else None,
            'RepetitionTimeExcitation': (
                5725.79052734375E-3 if site == 'HH' else
                8178.34E-3 if site == 'Guys' else
                None),
            'FlipAngle': 90 if site in ('HH', 'Guys') else None,
        },
    'MRA': lambda site: {
            'EchoTime': (
                5.75335741043090E-3 if site == 'HH' else
                6.9052E-3 if site == 'Guys' else
                None),
            'RepetitionTimeExcitation': (
                16.7210998535156E-3 if site == 'HH' else
                20E-3 if site == 'Guys' else
                None),
            'FlipAngle': (
                16 if site == 'HH' else
                25 if site == 'Guys' else
                None),
        },
    'DTI': lambda site: {
            'PulseSequenceType': 'Spin Echo EPI',
            'EchoTime': (
                51.0E-3 if site == 'HH' else
                80E-3 if site == 'Guys' else
                None),
            'RepetitionTimeExcitation': (
                11894.4384765625E-3 if site == 'HH' else
                9054.01E-3 if site == 'Guys' else
                None),
            'FlipAngle': 90 if site in ('HH', 'Guys') else None,
        },
}

modality_ixi_to_bids = {
    'T1': 'T1w',
    'T2': 'T2w',
    'PD': 'PDw',
    'MRA': 'angio',
    'DTI': 'dwi',
}


@click.command()
@click.option(
    '--path', default=None,
    help='Path to tree')
@click.option(
    '--key', multiple=True,
    type=click.Choice(["meta", "json", "T1", "T2", "PD", "MRA", "DTI"]),
    help='Only bidsify these keys')
@click.option(
    '--json-only', is_flag=True, default=False,
    help='Only write jsons (not volumes)'
)
def bidsify(path, key, json_only):
    logging.info('IXI - bidsify')
    keys = set(key or ['meta', 'json', 'T1', 'T2', 'PD', 'MRA', 'DTI'])
    path = path or '/autofs/space/pade_004/users/yb947/data4'
    ixipath = os.path.join(path, 'IXI')
    src = os.path.join(ixipath, 'sourcedata')
    raw = os.path.join(ixipath, 'rawdata')
    os.makedirs(raw, exist_ok=True)

    if 'meta' in keys:
        # The mapping from subject to site is only available through
        # individual filenames. We therefore need to first parse one of
        # the tar to build this mapping.
        if os.path.join(src, 'IXI-T1.tar'):
            sites = get_sites(os.path.join(src, 'IXI-T1.tar'))
        elif os.path.join(src, 'IXI-PD.tar'):
            sites = get_sites(os.path.join(src, 'IXI-PD.tar'))
        elif os.path.join(src, 'IXI-T2.tar'):
            sites = get_sites(os.path.join(src, 'IXI-T2.tar'))
        elif os.path.join(src, 'IXI-MRA.tar'):
            sites = get_sites(os.path.join(src, 'IXI-MRA.tar'))
        elif os.path.join(src, 'IXI-DTI.tar'):
            sites = get_sites(os.path.join(src, 'IXI-DTI.tar'))
        else:
            logging.error("No tar file available. Cannot compute sites.")

        logging.info('write README')
        with open(os.path.join(ixipath, 'README'), 'wt') as f:
            f.write(readme)

        logging.info('write dataset_description')
        jpath = os.path.join(ixipath, 'dataset_description.json')
        with open(jpath, 'wt') as f:
            json.dump(dataset_description_json, f, indent=2)

        logging.info('write participants')
        make_participants(
            os.path.join(src, 'IXI.xls'),
            os.path.join(ixipath, 'participants.tsv'),
            os.path.join(ixipath, 'participants.json'),
            sites,
        )

    # T1/T2/PD/MRA are simple "anat" scans that can be processed
    # identically.
    for key in keys.intersection(set(['T1', 'T2', 'PD', 'MRA'])):
        # check that the archive is available
        logging.info(f'process {key}')
        tarpath = os.path.join(src, f'IXI-{key}.tar')
        if not os.path.exists(tarpath):
            logging.warning(f'IXI-{key}.tar not found')
            continue
        # parse the archive
        modality = modality_ixi_to_bids[key]
        with tarfile.open(tarpath) as f:
            for member in f.getmembers():
                ixi_id, site, *_ = member.name.split('-')
                ixi_id = int(ixi_id[3:])
                subdir = os.path.join(raw, f'sub-{ixi_id:03d}', 'anat')
                os.makedirs(subdir, exist_ok=True)
                scanname = f'sub-{ixi_id:03d}_{modality}.nii.gz'
                jsonname = f'sub-{ixi_id:03d}_{modality}.json'
                if not json_only:
                    logging.info(f'write {scanname}')
                    with open(os.path.join(subdir, scanname), 'wb') as fo:
                        fo.write(f.extractfile(member).read())
                if 'json' in keys:
                    with open(os.path.join(subdir, jsonname), 'wt') as fo:
                        json.dump({
                            **scans_json(site),
                            **(modality_json[key](site))
                        }, fo, indent=2)

    # DWI scans are stored as individual 3D niftis (one per bval/bvec)
    # whereas BIDS prefers 4D niftis (actually 5D, since nifti specifies
    # that the 4th dimension is reserved for time)
    # We also need to deal with the bvals/bvecs files.
    if 'DTI' in keys:
        logging.info('process DTI')
        tarpath = os.path.join(src, 'IXI-DTI.tar')
        if not os.path.exists(tarpath):
            logging.warning('IXI-DTI.tar not found')
        else:
            # First, copy bvals/bvecs.
            # They are common to all subjects so we place them at the
            # top of the tree (under "rawdata/")
            if not os.path.exists(os.path.join(src, 'bvals.txt')):
                logging.error('bvals not found')
                return
            if not os.path.exists(os.path.join(src, 'bvecs.txt')):
                logging.error('bvecs not found')
                return
            shutil.copy(os.path.join(src, 'bvals.txt'),
                        os.path.join(raw, 'dwi.bval'))
            shutil.copy(os.path.join(src, 'bvecs.txt'),
                        os.path.join(raw, 'dwi.bvec'))
            # Then extract individual 3D volumes and save them with
            # temporary names (we use the non-BIDS-compliant "ch-{index}" tag)
            with tarfile.open(tarpath) as f:
                ixi_ids = {}
                for member in f.getmembers():
                    _, basename, _ = fileparts(member.name)
                    ixi_id, site, *_, dti_id = basename.split('-')
                    ixi_id = int(ixi_id[3:])
                    ixi_ids[ixi_id] = site
                    dti_id = int(dti_id)
                    subdir = os.path.join(raw, f'sub-{ixi_id:03d}', 'dwi')
                    os.makedirs(subdir, exist_ok=True)
                    scanname = f'sub-{ixi_id:03d}_ch-{dti_id:02d}_dwi.nii.gz'
                    if not json_only:
                        logging.info(f'write {scanname}')
                        with open(os.path.join(subdir, scanname), 'wb') as fo:
                            fo.write(f.extractfile(member).read())
            # Finally, go through each subject and combine the 3D b-series
            # into 5D niftis
            for ixi_id, site in ixi_ids.items():
                # if we only write JSON, do it separately
                if json_only:
                    jpath = os.path.join(subdir, f'sub-{ixi_id:03d}_dwi.json')
                    with open(jpath, 'wt') as fo:
                        json.dump({
                            **scans_json(site),
                            **(modality_json['DTI'](site))
                        }, fo, indent=2)
                    continue
                # now, concatenate volumes
                logging.info(f'concatenate sub-{ixi_id:03d}_dwi.nii.gz')
                subdir = os.path.join(raw, f'sub-{ixi_id:03d}', 'dwi')
                fnames = list(sorted(glob.glob(
                    os.path.join(subdir, f'sub-{ixi_id:03d}_ch-*_dwi.nii.gz')
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
                    logging.error(f'sub-{ixi_id:03d}_dwi | '
                                  f'shapes not compatible across channels')
                else:
                    # ensure 5D
                    dat = list(map(lambda x: x[..., None, None], dat))
                    # concatenate along the 5-th dimension (indexed 4)
                    # as the 4-th dimension is reserved for time
                    dat = np.concatenate(dat, axis=4)
                    nibabel.save(
                        nibabel.Nifti1Image(dat, affine, header),
                        os.path.join(subdir, f'sub-{ixi_id:03d}_dwi.nii.gz')
                    )
                    if 'json' in keys:
                        jpath = os.path.join(
                            subdir, f'sub-{ixi_id:03d}_dwi.json'
                        )
                        with open(jpath, 'wt') as fo:
                            json.dump({
                                **scans_json(site),
                                **(modality_json['DTI'](site))
                            }, fo, indent=2)
                for fname in fnames:
                    os.remove(fname)


def get_sites(path_tar):
    sitemap = {}
    with tarfile.open(path_tar) as f:
        for member in f.getmembers():
            ixi_id, site, *_ = member.name.split('-')
            ixi_id = int(ixi_id[3:])
            sitemap[ixi_id] = site
    return sitemap


def make_participants(path_xls, path_tsv, path_json, sites):
    import xlrd
    book = xlrd.open_workbook(path_xls)
    sheet = book.sheet_by_index(0)

    with open(path_json, 'wt') as f:
        json.dump(participants_json, f, indent=2)

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
    with open(path_tsv, 'wt') as fout:
        writer = csv.writer(fout, delimiter='\t', quoting=csv.QUOTE_NONE)
        writer.writerow(participants_header)
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
            writer.writerow(participant)


if __name__ == '__main__':

    root = logging.getLogger()
    root.setLevel(0)
    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(0)
    formatter = logging.Formatter('%(levelname)s | %(message)s')
    handler.setFormatter(formatter)
    root.addHandler(handler)

    bidsify()
