import os
import sys
import click
import tarfile
import logging
import csv
import nibabel
import numpy as np
from braindataprep.utils import (
    get_tree_path,
    fileparts,
    nibabel_convert,
    make_affine,
    relabel,
    copy_json,
    read_json,
    write_json,
    write_tsv,
    copy_from_buffer,
    write_from_buffer,
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

"""Folder containing template README/JSON/..."""
TPLDIR = os.path.join(os.path.dirname(__file__), 'templates')


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
    path = get_tree_path(path)
    keys = set(key or ['meta', 'raw', 'average', 'talairach', 'fsl', 'fs'])
    discs = set(disc or list(range(1, 13)))
    subs = sub
    oasispath = os.path.join(path, 'OASIS-1')
    src = os.path.join(oasispath, 'sourcedata')
    raw = os.path.join(oasispath, 'rawdata')
    drv = os.path.join(oasispath, 'derivatives')
    drvproc = os.path.join(drv, 'oasis-processed')
    drvfs = os.path.join(drv, 'oasis-freesurfer')

    # ------------------------------------------------------------------
    #   Write toplevel meta-information
    #    - README
    #    - dataset_description.json
    #    - participants.tsv
    #    - participants.json
    # ------------------------------------------------------------------
    if 'meta' in keys:

        copy_from_buffer(
            os.path.join(TPLDIR, 'README'),
            os.path.join(oasispath, 'README')
        )

        copy_json(
            os.path.join(TPLDIR, 'dataset_description.json'),
            os.path.join(oasispath, 'dataset_description.json')
        )

        copy_json(
            os.path.join(TPLDIR, 'participants.json'),
            os.path.join(oasispath, 'participants.json')
        )

        make_participants(
            os.path.join(src, 'oasis_cross-sectional.csv'),
            os.path.join(oasispath, 'participants.tsv'),
        )

        if 'fs' in keys:
            fs_bidsify_toplevel(drvfs, (4, 0))

    if not keys.intersection(set(['raw', 'n4', 'talairach', 'fsl', 'fs'])):
        return

    mprage_json = read_json(os.path.join(TPLDIR, 'T1w.json'))

    # OASIS data has an ASL layout
    # https://brainder.org/2011/08/13/converting-oasis-brains-to-nifti/
    affine_raw = make_affine(
        [256, 256, 128], [1.0, 1.0, 1.25], orient='ASL', center='x/2'
    )
    affine_avg = make_affine(
        [256, 256, 160], [1.0, 1.0, 1.0], orient='ASL', center='x/2'
    )
    affine_tal = make_affine(
        [176, 208, 176], [1.0, 1.0, 1.0], orient='LAS', center='x/2'
    )

    def tar2bids(f, tarpath, tarbase, dst, dstbase, json, affine):
        """Extract and convert tar member to BIDS"""
        write_json(json, os.path.join(dst, f'{dstbase}.json'))
        if json_only:
            return
        write_from_buffer(
            f.extractfile(f'{tarpath}/{tarbase}.hdr'),
            os.path.join(dst, f'{base}.hdr')
        )
        write_from_buffer(
            f.extractfile(f'{tarpath}/{tarbase}.img'),
            os.path.join(dst, f'{base}.img')
        )
        nibabel_convert(
            os.path.join(dst, f'{base}.img'),
            os.path.join(dst, f'{base}.nii.gz'),
            inp_format=nibabel.AnalyzeImage,
            remove=True,
            affine=affine,
        )

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
                if 'RAW' not in member.name:
                    continue
                if not member.name.endswith('.img'):
                    continue
                _, id, ses, run, _ = fileparts(member.name)[1].split('_')
                id, ses, run = int(id), int(ses[2:]), int(run[4:])
                if subs and id not in subs:
                    continue
                if ses != 1:
                    # skip repeats
                    continue
                subjects.setdefault(id, [])
                subjects[id].append(run)

            for id in subjects.keys():

                # ------------------------------------------------------
                #   Convert raw data
                #   (per-run scans only, the average is a derivative)
                #
                #   disc{d}/OAS1_{id}_MR1/RAW/*.img
                #   -> rawdata
                #   -> sub-{id}/anat/sub-{id}_run-{r}_T1w.nii.gz
                # ------------------------------------------------------
                if 'raw' in keys:
                    dst = os.path.join(raw, f'sub-{id:04d}', 'anat')
                    tarpath = f'disc{disc}/OAS1_{id:04d}_MR1/RAW'
                    for run in subjects[id]:
                        base = f'sub-{id:04d}_run-{run:d}_T1w'
                        tarbase = f'OAS1_{id:04d}_MR1_mpr-{run:d}_anon'
                        tar2bids(
                            f, tarpath, tarbase, dst, base,
                            mprage_json, affine_raw
                        )

                # ------------------------------------------------------
                #   Convert average scan
                #   (in derivative "oasis-processed")
                #
                #   disc{d}/OAS1_{id}_MR1/PROCESSED/MPRAGE/SUBJ_111/*.img
                #   -> derivatives/oasis-processed
                #   -> sub-{id}/anat/sub-{id}_res-1mm_T1w.nii.gz
                # ------------------------------------------------------
                if 'average' in keys:
                    dst = os.path.join(drvproc, f'sub-{id:04d}', 'anat')
                    base = f'sub-{id:04d}_res-1mm_T1w'
                    tarpath = f'disc{disc}/OAS1_{id:04d}_MR1'
                    tarpath = f'{tarpath}/PROCESSED/MPRAGE/SUBJ_111'
                    tarbase = f'OAS1_{id:04d}_MR1_mpr_n4_anon_sbj_111'
                    srcbase = f'bids:raw:sub-{id:04d}/anat/sub-{id:04d}'
                    bids_source = [
                        (srcbase + 'run-{run:d}_T1w.nii.gz').format(run=run)
                        for run in subjects[id]
                    ]
                    json = {
                        **mprage_json,
                        "SkullStripped": False,
                        "Resolution": "Resampled and averaged across runs "
                                      "(1mm, isotropic)",
                        "Sources": bids_source
                    }
                    tar2bids(
                        f, tarpath, tarbase, dst, base,
                        json, affine_avg,
                    )

                # ------------------------------------------------------
                #   Convert talairach-transformed scan
                #   (in derivative "oasis-processed")
                #
                #   disc{d}/OAS1_{id}_MR1/PROCESSED/MPRAGE/T88_111/*.img
                #   -> derivatives/oasis-processed
                #   -> sub-{id}/anat/sub-{id}_space-Talairach_res-1mm_T1w.nii.gz                        # noqa: E501
                #   -> sub-{id}/anat/sub-{id}_space-Talairach_res-1mm_desc-skullstripped_T1w.nii.gz     # noqa: E501
                # ------------------------------------------------------
                if 'talairach' in keys:
                    dst = os.path.join(drvproc, f'sub-{id:04d}', 'anat')
                    tarpath = f'disc{disc}/OAS1_{id:04d}_MR1'
                    tarpath = f'{tarpath}/PROCESSED/MPRAGE/T88_111'
                    srcbase = f'bids:raw:sub-{id:04d}/anat/sub-{id:04d}'
                    bids_source = [
                        (srcbase + 'run-{run:d}_T1w.nii.gz').format(run=run)
                        for run in subjects[id]
                    ]

                    # non-masked version
                    base = f'sub-{id:04d}_space-Talairach_res-1mm_T1w'
                    tarbase = f'OAS1_{id:04d}_MR1_mpr_n4_anon_111_t88_gfc'
                    json = {
                        **mprage_json,
                        "SkullStripped": False,
                        "Resolution": "Resampled and averaged across runs "
                                      "(1mm, isotropic)",
                        "Sources": bids_source
                    }
                    tar2bids(
                        f, tarpath, tarbase, dst, base,
                        json, affine_tal,
                    )

                    # masked version
                    flags = 'space-Talairach_res-1mm_desc-skullstripped'
                    base = f'sub-{id:04d}_{flags}_T1w'
                    tarbase = f'OAS1_{id:04d}_MR1'
                    tarbase = f'{tarbase}_mpr_n4_anon_111_t88_masked_gfc'
                    json = {
                        **mprage_json,
                        "SkullStripped": True,
                        "Resolution": "Resampled and averaged across runs "
                                      "(1mm, isotropic)",
                        "Sources": bids_source
                    }
                    tar2bids(
                        f, tarpath, tarbase, dst, base,
                        json, affine_tal,
                    )

                # ------------------------------------------------------
                #   Convert FSL segmentation
                #   (in derivative "oasis-processed")
                #
                #   disc{d}/OAS1_{id}_MR1/FSL_SEG/*.img
                #   -> derivatives/oasis-processed
                #   -> sub-{id}/anat/sub-{id}_space-Talairach_res-1mm_dseg.nii.gz  # noqa: E501
                # ------------------------------------------------------
                if 'fsl' in keys:
                    dst = os.path.join(drvproc, f'sub-{id:04d}', 'anat')
                    base = f'sub-{id:04d}_space-Talairach_res-1mm_dseg'
                    tarpath = f'disc{disc}/OAS1_{id:04d}_MR1/FSL_SEG'
                    tarbase = f'OAS1_{id:04d}_MR1'
                    tarbase = f'{tarbase}_mpr_n4_anon_111_t88_masked_gfc_fseg'
                    srcflags = 'space-Talairach_res-1mm_desc-skullstripped'
                    json = {
                        "Manual": "False",
                        "Resolution": "In the space of the 1mm Talairach T1w "
                                      "scan (1mm, isotropic)",
                        "Sources": [
                            f"bids::sub-{id:04d}/anat/"
                            f"sub-{id:04d}_{srcflags}_T1w.nii.gz",
                        ]
                    }
                    tar2bids(
                        f, tarpath, tarbase, dst, base,
                        json, affine_tal,
                    )
                    # relabel using BIDS indexing scheme
                    volf = nibabel.load(
                        os.path.join(dst, f'{base}.nii.gz')
                    )
                    vold = relabel(
                        np.asarray(volf.dataobj), {1: 2, 2: 3, 3: 1}
                    )
                    nibabel.save(
                        type(volf)(vold, volf.affine, volf.header),
                        os.path.join(dst, f'{base}.nii.gz')
                    )

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

        # --------------------------------------------------------------
        #   Save all subject ids and unpack the raw freesurfer outputs
        #   under "derivatives/oasis-freesurfer/sourcedata/sub-{04d}"
        # --------------------------------------------------------------
        ids = set()
        with tarfile.open(tarpath, 'r:gz') as f:
            for member in f.getmembers():
                if not all_fs:
                    if not member.name.endswith(fs_bidsifiable_outputs):
                        continue
                _, id, fsdir, *basename = member.name.split('/')
                _, id, ses = id.split('_')
                id, ses = int(id), int(ses[2:])
                if subs and id not in subs:
                    continue
                if ses != 1:
                    # skip repeats
                    continue
                ids.add(id)
                dst = os.path.join(drvfs, 'sourcedata', f'sub-{id:04d}')
                scanname = os.path.join(dst, fsdir, *basename)
                copy_from_buffer(f.extractfile(member), scanname)

        # --------------------------------------------------------------
        #   Bidsify each subject
        # --------------------------------------------------------------
        for id in ids:
            outsubdir = os.path.join(drvfs, f'sub-{id:04d}')
            inpsubdir = os.path.join(drvfs, 'sourcedata', f'sub-{id:04d}')
            srcbase = f'bids:raw:sub-{id:04d}/anat/sub-{id:04d}'
            sourcefiles = [
                srcbase + '_run-{:d}_T1w.nii.gz'.format(run)
                for run in (1, 2, 3, 4)
            ]
            fs_bidsify(inpsubdir, outsubdir, sourcefiles)


def make_participants(path_csv, path_tsv):

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

    def iter_rows():
        with open(path_csv, 'rt') as finp:
            yield participants_header
            reader = csv.reader(finp, delimiter=',', quoting=csv.QUOTE_NONE)
            next(reader)  # skip header
            for row in reader:
                id, *values = row
                values = values[:-1]  # remove delay column
                values = ["n/a" if v in ('', 'N/A') else v for v in values]
                _, id, ses = id.split('_')
                id, ses = int(id), int(ses[2:])
                if ses == 2:
                    continue
                yield [f'sub-{id:04d}', *values]

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
