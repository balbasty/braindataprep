import os
import sys
import click
import tarfile
import logging
import csv
import json
import nibabel
import numpy as np
from braindataprep.utils import (
    fileparts, nibabel_convert, make_affine, relabel
)
from braindataprep.freesurfer.bidsify import (
    bidsify as fs_bidsify,
    bidsify_toplevel as fs_bidsify_toplevel,
    bidsifiable_outputs as fs_bidsifiable_outputs,
)

"""
Expected input
--------------
OASIS-I/
    sourcedata/
        oasis_cross-sectional_disc{1..12}.tar.gz
        oasis_cs_freesurfer_disc{1..12}.tar.gz
        oasis_cross-sectional.csv
        oasis_cross-sectional-reliability.csv
        oasis_cross-sectional_facts.pdf

Expected output
---------------
OASIS-I/
    dataset_description.json
    participants.tsv
    participants.json
    rawdata/
        sub-{04d}/
            anat/
                sub-{04d}_run-{d}_T1w.{nii.gz|json}
    derivatives/
        oasis-processed/
            sub-{04d}/
                anat/
                    sub-{04d}_res-1mm_T1w.{nii.gz|json}
                    sub-{04d}_space-Talairach_res-1mm_T1w.{nii.gz|json}
                    sub-{04d}_space-Talairach_res-1mm_desc-skullstripped_T1w.{nii.gz|json}
                    sub-{04d}_space-Talairach_res-1mm_dseg.{nii.gz|json}
        oasis-freesurfer/
            sub-{04d}/
                anat/
                    sub-{04d}_desc-orig_T1w.{nii.gz|json}
                    sub-{04d}_res-1mm_desc-orig_T1w.{nii.gz|json}
                    sub-{04d}_res-1mm_desc-norm_T1w.{nii.gz|json}
                    sub-{04d}_atlas-Aseg_dseg.{nii.gz|json}
                    sub-{04d}_atlas-AsegDesikanKilliany_dseg.{nii.gz|json}
                    sub-{04d}_hemi-L_wm.surf.{gii|json}
                    sub-{04d}_hemi-L_pial.surf.{gii|json}
                    sub-{04d}_hemi-L_smoothwm.surf.{gii|json}
                    sub-{04d}_hemi-L_inflated.surf.{gii|json}
                    sub-{04d}_hemi-L_sphere.surf.{gii|json}
                    sub-{04d}_hemi-L_curv.shape.{gii|json}
                    sub-{04d}_hemi-L_sulc.shape.{gii|json}
                    sub-{04d}_hemi-L_thickness.shape.{gii|json}
                    sub-{04d}_hemi-L_desc-wm_area.shape.{gii|json}
                    sub-{04d}_hemi-L_desc-pial_area.shape.{gii|json}
                    sub-{04d}_hemi-L_atlas-DesikanKilliany_dseg.label.{gii|json}
                    sub-{04d}_hemi-R_wm.surf.{gii|json}
                    sub-{04d}_hemi-R_pial.surf.{gii|json}
                    sub-{04d}_hemi-R_smoothwm.surf.{gii|json}
                    sub-{04d}_hemi-R_inflated.surf.{gii|json}
                    sub-{04d}_hemi-R_sphere.surf.{gii|json}
                    sub-{04d}_hemi-R_curv.shape.{gii|json}
                    sub-{04d}_hemi-R_sulc.shape.{gii|json}
                    sub-{04d}_hemi-R_thickness.shape.{gii|json}
                    sub-{04d}_hemi-R_desc-wm_area.shape.{gii|json}
                    sub-{04d}_hemi-R_desc-pial_area.shape.{gii|json}
                    sub-{04d}_hemi-R_atlas-DesikanKilliany_dseg.label.{gii|json}
"""

readme = """
# OASIS-I: Cross-sectional data across the adult lifespan (Marcus et al., 2007)

## Quotes from the original fact sheet

### Sumary

OASIS provides brain imaging data that are freely available for distribution
and data analysis. This data set consists of a cross-sectional collection of
416 subjects covering the adult life span aged 18 to 96 including individuals
with early-stage Alzheimer's Disease (AD). For each subject, 3 or 4 individual
T1-weighted MRI scans obtained within a single imaging session are included.
The subjects are all right-handed and include both men and women. 100 of the
included subjects over the age of 60 have been diagnosed with very mild to
mild AD. Additionally, for 20 of the nondemented subjects, images from a
subsequent scan session after a short delay (less than 90 days) are also
included as a means of assessing acquisition reliability.

All data have been anonymized to accommodate public distribution.
Facial features were removed at the fMRIDC (http://www.fmridc.org)
using the Brain Extraction Tool.

The full set is 15.8 GB compressed and 50 GB uncompressed.
The data are available at http://www.oasis-brains.org.

### Image Data

For each subject, a number of images are provided, including:

    1.  3-4 images corresponding to multiple repetitions of the same
        structural protocol within a single session to increase
        signal-to-noise,
    2.  an average image that is a motion-corrected coregistered average
        of all available data,
    3.  a gain-field corrected atlasregistered image to the 1988 atlas
        space of Talairach and Tournoux (Buckner et al., 2004),
    4.  a masked version of the atlas-registered image in which all
        non-brain voxels have been assigned an intensity value of 0, and
    5.  a grey/white/CSF segmented image (Zhang et al., 2001).
All images are in 16-bit big-endian Analyze 7.5 format.

"""

"""
JSONs
"""
dataset_description_json = {
    'Name': 'OASIS-I',
    'BIDSVersion': '1.8.0',
    'Authors': [
        'Marcus, D',
        'Buckner, R',
        'Csernansky, J',
        'Morris, J'
    ],
    'Funding': [
        'P50 AG05681',
        'P01 AG03991',
        'P01 AG026276',
        'R01 AG021910',
        'P20 MH071616',
        'U24 RR021382'
    ],
    'ReferencesAndLinks': ['https://doi.org/10.1162/jocn.2007.19.9.1498'],
    'HowToAcknowledge': """
You will acknowledge the use of OASIS data and data derived from OASIS
data when publicly presenting any results or algorithms that benefitted from
their use. Papers, book chapters, books, posters, oral presentations, and
all other printed and digital presentations of results derived from OASIS
data should contain the following:

* Acknowledgements:
    "Data were provided by OASIS: [insert appropriate OASIS source info]”
    Principal Investigators:  D. Marcus, R, Buckner, J, Csernansky J. Morris;
    P50 AG05681, P01 AG03991, P01 AG026276, R01 AG021910, P20 MH071616, U24 RR021382

* Citation:
    https://doi.org/10.1162/jocn.2007.19.9.1498
"""  # noqa: E501
}

participants_json = {
    "sex": {
        "Description": "Sex of the participant as reported by the participant",
        "Levels": {
            "M": "male",
            "F": "female"
        }
    },
    "handedness": {
        "Description": "Handedness of the participant",
        "Levels": {
            "L": "left",
            "R": "right"
        }
    },
    "age": {
        "Description": "Age of the participant at time of image acquisition",
        "Units": "years"
    },
    "educ": {
        "Description": "Education",
        "Levels": {
            "1": "less than high school grad.",
            "2": "high school grad.",
            "3": "some college",
            "4": "college grad.",
            "5": "beyond college"
        }
    },
    "ses": {
        "Description": "Socioeconomic status, assessed by the Hollingshead "
                       "Index of Social Position and classified into "
                       "categories from 1 (highest status) to "
                       "5 (lowest status) (Hollingshead, 1957).",
        "Levels": {
            "1": "highest socioeconomic status.",
            "2": "",
            "3": "",
            "4": "",
            "5": "lowest socioeconomic status"
        }
    },
    "mmse": {
        "Description": "Mini-Mental State Examination. "
                       "Ranges from 0 (worst) to 30 (best) "
                       "(Folstein, Folstein, & McHugh, 1975)",
    },
    "cdr": {
        "Description": "Clinical Dementia Rating (Morris, 1993). "
                       "All participants with dementia (CDR >0) "
                       "were diagnosed with probable AD.",
        "Levels": {
            "0": "no dementia",
            "0.5": "very mild AD",
            "1": "mild AD",
            "2": "moderate AD"
        },
    },
    "etiv": {
        "Description": "Estimated Total Intracranial Volume",
        "Unit": "cubic millimeter"
    },
    "nwbv": {
        "Description": "Normalized Whole Brain Volume (Fotenos et al., 2004)",
        "Unit": "unitless"
    },
    "asf": {
        "Description": "Atlas Scaling Factor (Buckner et al., 2004)",
        "Unit": "unitless"
    },
}

mprage_json = {
    'Manufacturer': 'Siemens',
    'MagneticFieldStrength': 1.5,
    'PulseSequenceType': 'MPRAGE',
    'EchoTime': 4E-3,
    'InversionTime': 20E-3,
    'RepetitionTimeExcitation': 9.7E-3,
    'RepetitionTimePreparation': 200E-3,
    'FlipAngle': 10,
}


@click.command()
@click.option(
    '--path', default=None, help='Path to tree')
@click.option(
    '--key', multiple=True,
    type=click.Choice(["meta", "raw", "average", "talairach", "fsl", "fs"]),
    help='Only download these keys')
@click.option(
    '--disc', multiple=True, type=int,
    help='Only download these discs (1..12)')
@click.option(
    '--sub', multiple=True, type=int,
    help='Only download these subjects')
@click.option(
    '--all-fs', is_flag=True, default=False,
    help='Unpack all FS sourcedata (even files that cannot be bidsified)')
@click.option(
    '--json-only', is_flag=True, default=False,
    help='Only write jsons (not volumes)'
)
def bidsify(path, key, disc, sub, all_fs, json_only):
    logging.info('OASIS-I - bidsify')
    keys = set(key or ['meta', 'raw', 'average', 'talairach', 'fsl', 'fs'])
    discs = set(disc or list(range(1, 13)))
    subs = sub
    path = path or '/autofs/space/pade_004/users/yb947/data4'
    oasispath = os.path.join(path, 'OASIS-1')
    src = os.path.join(oasispath, 'sourcedata')
    raw = os.path.join(oasispath, 'rawdata')
    drv = os.path.join(oasispath, 'derivatives')
    drvfs = os.path.join(drv, 'oasis-freesurfer')

    # ------------------------------------------------------------------
    #   Write toplevel meta-information
    #    - README
    #    - dataset_description.json
    #    - participants.tsv
    #    - participants.json
    # ------------------------------------------------------------------
    if 'meta' in keys:

        logging.info('write README')
        with open(os.path.join(oasispath, 'README'), 'wt') as f:
            f.write(readme)

        logging.info('write dataset_description')
        jpath = os.path.join(oasispath, 'dataset_description.json')
        with open(jpath, 'wt') as f:
            json.dump(dataset_description_json, f, indent=2)

        logging.info('write participants')
        make_participants(
            os.path.join(src, 'oasis_cross-sectional.csv'),
            os.path.join(oasispath, 'participants.tsv'),
            os.path.join(oasispath, 'participants.json'),
        )

        if 'fs' in keys:
            logging.info('write freesurfer metadata')
            os.makedirs(drvfs, exist_ok=True)
            fs_bidsify_toplevel(drvfs, (4, 0))

    if not keys.intersection(set(['raw', 'n4', 'talairach', 'fsl', 'fs'])):
        return

    # ------------------------------------------------------------------
    #   Write toplevel meta-information
    #    - README
    #    - dataset_description.json
    #    - participants.tsv
    #    - participants.json
    # ------------------------------------------------------------------
    for disc in discs:

        # --------------------------------------------------------------
        #   Convert raw and minimally processed data
        #   from "oasis_cross-sectional_disc{d}.tar.gz"
        # --------------------------------------------------------------

        logging.info(f'process disc {disc}')
        tarpath = os.path.join(src, f'oasis_cross-sectional_disc{disc}.tar.gz')
        if not os.path.exists(tarpath):
            logging.warning(
                f'oasis_cross-sectional_disc{disc}.tar.gz not found'
            )
            continue

        with tarfile.open(tarpath, 'r:gz') as f:
            # ----------------------------------------------------------
            #   Save all subject ids and runs contained in this disc
            # ----------------------------------------------------------
            subjects = {}
            for member in f.getmembers():
                if 'RAW' not in member.name or not member.name.endswith('.img'):
                    continue
                _, basename, _ = fileparts(member.name)
                _, oasis_id, ses, run, _ = basename.split('_')
                oasis_id, ses, run = int(oasis_id), int(ses[2:]), int(run[4:])
                if subs and oasis_id not in subs:
                    continue
                if ses != 1:
                    # skip repeats
                    continue
                subjects.setdefault(oasis_id, [])
                subjects[oasis_id].append(run)

            # ----------------------------------------------------------
            #   Convert raw data
            #   (per-run scans only, the average is a derivative)
            # ----------------------------------------------------------
            if 'raw' in keys:

                # OASIS data has an ASL layout
                # https://brainder.org/2011/08/13/converting-oasis-brains-to-nifti/
                affine = make_affine([256, 256, 128], [1.0, 1.0, 1.25],
                                     orient='ASL', center='x/2')

                for oasis_id, runs in subjects.items():
                    subdir = os.path.join(raw, f'sub-{oasis_id:04d}', 'anat')
                    os.makedirs(subdir, exist_ok=True)
                    for run in runs:
                        hdrname = f'sub-{oasis_id:04d}_run-{run:d}_T1w.hdr'
                        imgname = f'sub-{oasis_id:04d}_run-{run:d}_T1w.img'
                        scanname = f'sub-{oasis_id:04d}_run-{run:d}_T1w.nii.gz'
                        jsonname = f'sub-{oasis_id:04d}_run-{run:d}_T1w.json'
                        oasisraw = f'disc{disc}/OAS1_{oasis_id:04d}_MR1/RAW'
                        logging.info(f'write {scanname}')
                        with open(os.path.join(subdir, jsonname), 'wt') as fo:
                            json.dump(mprage_json, fo, indent=2)
                        if not json_only:
                            with open(os.path.join(subdir, hdrname), 'wb') as fo:
                                fname = f'{oasisraw}/OAS1_{oasis_id:04d}_MR1_mpr-{run}_anon.hdr'
                                fo.write(f.extractfile(fname).read())
                            with open(os.path.join(subdir, imgname), 'wb') as fo:
                                fname = f'{oasisraw}/OAS1_{oasis_id:04d}_MR1_mpr-{run}_anon.img'
                                fo.write(f.extractfile(fname).read())
                            nibabel_convert(
                                os.path.join(subdir, imgname),
                                os.path.join(subdir, scanname),
                                inp_format=nibabel.AnalyzeImage,
                                remove=True,
                                affine=affine,
                            )

            # ----------------------------------------------------------
            #   Convert average scan
            #   (in derivative "oasis-processed")
            # ----------------------------------------------------------
            if 'average' in keys:
                affine = make_affine([256, 256, 160], [1.0, 1.0, 1.0],
                                     orient='ASL', center='x/2')

                for oasis_id, runs in subjects.items():
                    subdir = os.path.join(
                        drv, 'oasis-processed', f'sub-{oasis_id:04d}', 'anat'
                    )
                    os.makedirs(subdir, exist_ok=True)
                    hdrname = f'sub-{oasis_id:04d}_res-1mm_T1w.hdr'
                    imgname = f'sub-{oasis_id:04d}_res-1mm_T1w.img'
                    scanname = f'sub-{oasis_id:04d}_res-1mm_T1w.nii.gz'
                    jsonname = f'sub-{oasis_id:04d}_res-1mm_T1w.json'
                    logging.info(f'write {scanname}')
                    with open(os.path.join(subdir, jsonname), 'wt') as fo:
                        json.dump({
                            **mprage_json,
                            "SkullStripped": False,
                            "Resolution": "Resampled and averaged across runs (1mm, isotropic)",
                            "Sources": [
                                "bids:raw:sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_run-1_T1w.nii.gz}",
                                "bids:raw:sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_run-2_T1w.nii.gz}",
                                "bids:raw:sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_run-3_T1w.nii.gz}",
                                "bids:raw:sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_run-4_T1w.nii.gz}",
                            ],
                        }, fo, indent=2)
                    if not json_only:
                        with open(os.path.join(subdir, hdrname), 'wb') as fo:
                            fname = f'disc{disc}/OAS1_{oasis_id:04d}_MR1/PROCESSED/MPRAGE/SUBJ_111/OAS1_{oasis_id:04d}_MR1_mpr_n4_anon_sbj_111.hdr'
                            fo.write(f.extractfile(fname).read())
                        with open(os.path.join(subdir, imgname), 'wb') as fo:
                            fname = f'disc{disc}/OAS1_{oasis_id:04d}_MR1/PROCESSED/MPRAGE/SUBJ_111/OAS1_{oasis_id:04d}_MR1_mpr_n4_anon_sbj_111.img'
                            fo.write(f.extractfile(fname).read())
                        nibabel_convert(
                            os.path.join(subdir, imgname),
                            os.path.join(subdir, scanname),
                            inp_format=nibabel.AnalyzeImage,
                            remove=True,
                            affine=affine,
                        )

            # ----------------------------------------------------------
            #   Convert talairach-transformed scan
            #   (in derivative "oasis-processed")
            # ----------------------------------------------------------
            if 'talairach' in keys:
                affine = make_affine([176, 208, 176], [1.0, 1.0, 1.0],
                                     orient='LAS', center='x/2')

                for oasis_id, runs in subjects.items():
                    subdir = os.path.join(drv, 'oasis-processed', f'sub-{oasis_id:04d}', 'anat')
                    os.makedirs(subdir, exist_ok=True)

                    # non-masked version
                    hdrname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_T1w.hdr'
                    imgname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_T1w.img'
                    scanname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_T1w.nii.gz'
                    jsonname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_T1w.json'
                    logging.info(f'write {scanname}')
                    with open(os.path.join(subdir, jsonname), 'wt') as fo:
                        json.dump({
                            **mprage_json,
                            "SkullStripped": False,
                            "Resolution": "Resampled and averaged across runs (1mm, isotropic)",
                            "Sources": [
                                "bids:raw:sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_run-1_T1w.nii.gz}",
                                "bids:raw:sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_run-2_T1w.nii.gz}",
                                "bids:raw:sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_run-3_T1w.nii.gz}",
                                "bids:raw:sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_run-4_T1w.nii.gz}",
                            ],
                        }, fo, indent=2)
                    if not json_only:
                        with open(os.path.join(subdir, hdrname), 'wb') as fo:
                            fname = f'disc{disc}/OAS1_{oasis_id:04d}_MR1/PROCESSED/MPRAGE/T88_111/OAS1_{oasis_id:04d}_MR1_mpr_n4_anon_111_t88_gfc.hdr'
                            fo.write(f.extractfile(fname).read())
                        with open(os.path.join(subdir, imgname), 'wb') as fo:
                            fname = f'disc{disc}/OAS1_{oasis_id:04d}_MR1/PROCESSED/MPRAGE/T88_111/OAS1_{oasis_id:04d}_MR1_mpr_n4_anon_111_t88_gfc.img'
                            fo.write(f.extractfile(fname).read())
                        nibabel_convert(
                            os.path.join(subdir, imgname),
                            os.path.join(subdir, scanname),
                            inp_format=nibabel.AnalyzeImage,
                            remove=True,
                            affine=affine,
                        )
                    # masked version
                    hdrname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_desc-skullstripped_T1w.hdr'
                    imgname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_desc-skullstripped_T1w.img'
                    scanname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_desc-skullstripped_T1w.nii.gz'
                    jsonname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_desc-skullstripped_T1w.json'
                    logging.info(f'write {scanname}')
                    with open(os.path.join(subdir, jsonname), 'wt') as fo:
                        json.dump({
                            **mprage_json,
                            "SkullStripped": True,
                            "Resolution": "Resampled and averaged across runs (1mm, isotropic)",
                            "Sources": [
                                "bids::sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_res-1mm_T1w.nii.gz}",
                            ],
                        }, fo, indent=2)
                    if not json_only:
                        with open(os.path.join(subdir, hdrname), 'wb') as fo:
                            fname = f'disc{disc}/OAS1_{oasis_id:04d}_MR1/PROCESSED/MPRAGE/T88_111/OAS1_{oasis_id:04d}_MR1_mpr_n4_anon_111_t88_masked_gfc.hdr'
                            fo.write(f.extractfile(fname).read())
                        with open(os.path.join(subdir, imgname), 'wb') as fo:
                            fname = f'disc{disc}/OAS1_{oasis_id:04d}_MR1/PROCESSED/MPRAGE/T88_111/OAS1_{oasis_id:04d}_MR1_mpr_n4_anon_111_t88_masked_gfc.img'
                            fo.write(f.extractfile(fname).read())
                        nibabel_convert(
                            os.path.join(subdir, imgname),
                            os.path.join(subdir, scanname),
                            inp_format=nibabel.AnalyzeImage,
                            remove=True,
                            affine=affine,
                        )

            # ----------------------------------------------------------
            #   Convert FSL segmentation
            #   (in derivative "oasis-processed")
            # ----------------------------------------------------------
            if 'fsl' in keys:
                affine = make_affine([176, 208, 176], [1.0, 1.0, 1.0],
                                     orient='LAS', center='x/2')

                for oasis_id, runs in subjects.items():
                    subdir = os.path.join(drv, 'oasis-processed', f'sub-{oasis_id:04d}', 'anat')
                    os.makedirs(subdir, exist_ok=True)

                    # masked version
                    hdrname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_dseg.hdr'
                    imgname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_dseg.img'
                    scanname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_dseg.nii.gz'
                    jsonname = f'sub-{oasis_id:04d}_space-Talairach_res-1mm_dseg.json'
                    logging.info(f'write {scanname}')
                    with open(os.path.join(subdir, jsonname), 'wt') as fo:
                        json.dump({
                            "Manual": "False",
                            "Resolution": "In the space of the 1mm Talairach T1w scan (1mm, isotropic)",
                            "Sources": [
                                "bids::sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_space-Talairach_res-1mm_desc-skullstripped_T1w.nii.gz}",
                            ],
                        }, fo, indent=2)
                    if not json_only:
                        with open(os.path.join(subdir, hdrname), 'wb') as fo:
                            fname = f'disc{disc}/OAS1_{oasis_id:04d}_MR1/PROCESSED/MPRAGE/T88_111/OAS1_{oasis_id:04d}_MR1_mpr_n4_anon_111_t88_masked_gfc.hdr'
                            fo.write(f.extractfile(fname).read())
                        with open(os.path.join(subdir, imgname), 'wb') as fo:
                            fname = f'disc{disc}/OAS1_{oasis_id:04d}_MR1/PROCESSED/MPRAGE/T88_111/OAS1_{oasis_id:04d}_MR1_mpr_n4_anon_111_t88_masked_gfc.img'
                            fo.write(f.extractfile(fname).read())
                        nibabel_convert(
                            os.path.join(subdir, imgname),
                            os.path.join(subdir, scanname),
                            inp_format=nibabel.AnalyzeImage,
                            remove=True,
                            affine=affine,
                        )
                        # relabel using BIDS indexing scheme
                        volf = nibabel.load(os.path.join(subdir, scanname))
                        vold = relabel(np.asarray(volf.dataobj), {1: 2, 2: 3, 3: 1})
                        nibabel.save(type(volf)(vold, volf.affine, volf.header),
                                     os.path.join(subdir, scanname))

        if 'fs' not in keys:
            continue

        # --------------------------------------------------------------
        #   Convert FreeSurfer processed data
        #   from "oasis_cs_freesurfer_disc{d}.tar.gz"
        #
        #   I could not find which FS version was used, but it writes
        #   the old Destrieux parcellation (2005), so is strictly
        #   older than 4.5
        # --------------------------------------------------------------

        tarpath = os.path.join(src, f'oasis_cs_freesurfer_disc{disc}.tar.gz')
        if not os.path.exists(tarpath):
            logging.warning(
                f'oasis_cs_freesurfer_disc{disc}.tar.gz not found'
            )
            continue
        os.makedirs(os.path.join(drv, 'oasis-freesurfer'), exist_ok=True)

        # --------------------------------------------------------------
        #   Save all subject ids and unpack the raw freesurfer outputs
        #   under "derivatives/oasis-freesurfer/sourcedata/sub-{04d}"
        # --------------------------------------------------------------
        oasis_ids = set()
        with tarfile.open(tarpath, 'r:gz') as f:
            for member in f.getmembers():
                if not all_fs and not member.name.endswith(fs_bidsifiable_outputs):
                    continue
                _, oasis_id, fsdir, *basename = member.name.split('/')
                _, oasis_id, ses = oasis_id.split('_')
                oasis_id, ses = int(oasis_id), int(ses[2:])
                if subs and oasis_id not in subs:
                    continue
                if ses != 1:
                    # skip repeats
                    continue
                oasis_ids.add(oasis_id)
                subdir = os.path.join(drv, 'oasis-freesurfer', 'sourcedata', f'sub-{oasis_id:04d}')
                os.makedirs(os.path.join(subdir, fsdir, *(basename[:-1])), exist_ok=True)
                scanname = os.path.join(subdir, fsdir, *basename)
                logging.info(f'write {os.path.join(f"sub-{oasis_id:04d}", *basename)}')
                with open(scanname, 'wb') as fo:
                    fo.write(f.extractfile(member).read())

        # --------------------------------------------------------------
        #   Bidsify each subject
        # --------------------------------------------------------------
        for oasis_id in oasis_ids:
            outsubdir = os.path.join(drv, 'oasis-freesurfer', f'sub-{oasis_id:04d}')
            inpsubdir = os.path.join(drv, 'oasis-freesurfer', 'sourcedata', f'sub-{oasis_id:04d}')
            sourcefiles = [
                'bids:raw:sub-{oasis_id:04d}/anat/sub-{oasis_id:04d}_run-{run:d}_T1w.nii.gz'.format(
                    oasis_id=oasis_id, run=run
                )
                for run in range(1, 5)
            ]
            fs_bidsify(inpsubdir, outsubdir, sourcefiles)


def get_sites(path_tar):
    sitemap = {}
    with tarfile.open(path_tar) as f:
        for member in f.getmembers():
            ixi_id, site, *_ = member.name.split('-')
            ixi_id = int(ixi_id[3:])
            sitemap[ixi_id] = site
    return sitemap


def make_participants(path_csv, path_tsv, path_json):
    with open(path_json, 'wt') as f:
        json.dump(participants_json, f, indent=2)

    """
    oasis_header = [
        'ID',
        'M/F',
        'Hand',
        'Age',
        'Educ',
        'SES',
        'MMSE',
        'CDR',
        'eTIV',
        'nWBV',
        'ASF',
        'Delay'
    ]
    """

    participants_header = [
        'participant_id',
        'sex',
        'handedness'
        'age',
        'educ',
        'ses',
        'mmse',
        'cdr',
        'etiv',
        'nwbv',
        'asf',
    ]
    with open(path_csv, 'rt') as finp, open(path_tsv, 'wt') as fout:
        reader = csv.reader(finp, delimiter=',', quoting=csv.QUOTE_NONE)
        writer = csv.writer(fout, delimiter='\t', quoting=csv.QUOTE_NONE)
        next(reader)
        writer.writerow(participants_header)
        for row in reader:
            oasis_id, *values = row
            values = values[:-1]  # remove delay column
            values = ["n/a" if v in ('', 'N/A') else v for v in values]
            _, oasis_id, ses = oasis_id.split('_')
            oasis_id, ses = int(oasis_id), int(ses[2:])
            if ses == 2:
                continue
            participant = [f'sub-{oasis_id:04d}', *values]
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
