"""
Expected input
--------------
OASIS-3/
    sourcedata/
        OASIS3_data_files/
            # demographics + cognitive tests
            demo.tar.gz
            dictionaries.tar.gz
            # Modality-specific BIDS json
            MRI-json.tar.gz
            CT-json.tar.gz
            PET-json.tar.gz
            # derivatives
            FS.tar.gz
            PUP.tar.gz
            # UDS
            UDSa1.tar.gz
            UDSa2.tar.gz
            UDSa3.tar.gz
            UDSa4d.tar.gz
            UDSa4g.tar.gz
            UDSa5.tar.gz
            UDSb1.tar.gz
            UDSb2.tar.gz
            UDSb3.tar.gz
            UDSb4.tar.gz
            UDSb5.tar.gz
            UDSb6.tar.gz
            UDSb7.tar.gz
            UDSb8.tar.gz
            UDSb9.tar.gz
            pychometrics.tar.gz  (== UDSc1)
            UDSd1.tar.gz
            UDSd2.tar.gz
        OAS3{04d}_MR_d{04d}/
            anat{d}.tar.gz
            func{d}.tar.gz
            fmap{d}.tar.gz
            dwi{d}.tar.gz
            swi{d}.tar.gz
        OAS3{04d}_CT_d{04d}/
            CT{d}.tar.gz
        OAS3{04d}_PIB_d{04d}/
            pet{d}.tar.gz
        OAS3{04d}_AV45_d{04d}/
            pet{d}.tar.gz

Expected output
---------------
OASIS-3/
    dataset_description.json
    participants.{tsv|json}
    sessions.json
    phenotypes/
        UDSv2_a1_demographics.{tsv|json}
        UDSv2_a2_informant.{tsv|json}
        UDSv2_a3_family_history.{tsv|json}
        UDSv2_a4_medications.{tsv|json}
        UDSv2_a5_health_history.{tsv|json}
        UDSv2_b1_physical.{tsv|json}
        UDSv2_b2_hiscvd.{tsv|json}
        UDSv2_b3_updrs.{tsv|json}
        UDSv2_b4_cdr.{tsv|json}
        UDSv2_b5_npiq.{tsv|json}
        UDSv2_b6_gds.{tsv|json}
        UDSv2_b7_fas.{tsv|json}
        UDSv2_b8_neurofind.{tsv|json}
        UDSv2_b9_symptoms.{tsv|json}
        UDSv2_c1_neuropsy.{tsv|json}
        UDSv2_d1_diagnosis.{tsv|json}
        UDSv2_d2_medical_conditions.{tsv|json}
    rawdata/
        sub-{04d}/
            sub-{04d}_sessions.tsv
            ses-{d}/
                anat/
                    sub-{:04d}_ses-{:04d}_T1w.{nii.gz|json}
                    sub-{:04d}_ses-{:04d}_T2w.{nii.gz|json}
                    sub-{:04d}_ses-{:04d}_acq-TSE_T2w.{nii.gz|json}
                    sub-{:04d}_ses-{:04d}_FLAIR.{nii.gz|json}
                    sub-{:04d}_ses-{:04d}_T2star.{nii.gz|json}
                    sub-{:04d}_ses-{:04d}_angio.{nii.gz|json}
                    sub-{:04d}_ses-{:04d}_swi.{nii.gz|json}
                perf/
                    sub-{:04d}_ses-{:04d}_pasl.nii.gz
                func/
                    sub-{:04d}_ses-{:04d}_task-rest*_run-{:02d}_bold.nii.gz
                fmap/
                    sub-{:04d}_ses-{:04d}_echo-1_run-01_fieldmap.nii.gz
                dwi/
                    sub-{:04d}_ses-{:04d}_run-{:02d}_dwi.nii.gz
                pet/
                    sub-{:04d}_trc-PIB_pet.{nii.gz|json}
                    sub-{:04d}_trc-AV45_pet.{nii.gz|json}
"""
from pathlib import Path
from typing import Literal, Iterable

from braindataprep.utils.log import setup_filelog
from braindataprep.utils.path import get_tree_path
from braindataprep.actions.action import IfExists
from braindataprep.datasets.OASIS.III.command import oasis3
from braindataprep.datasets.OASIS.III.bidsifier import Bidsifier
from braindataprep.datasets.OASIS.III.keys import allleaves


DATASET = 'OASIS-3'


@oasis3.command
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Iterable[str] = tuple(),
    exclude_keys: Iterable[str] | None = tuple(),
    subs: Iterable[int] | None = tuple(),
    exclude_subs: Iterable[int] | None = tuple(),
    json: Literal["yes", "no", "only"] | bool = "yes",
    if_exists: IfExists.Choice = "skip",
    log: str | None = None,
):
    """
    Convert source data into a BIDS-compliant directory

    **Hierarchy of keys:**

    * raw :              All the raw imaging data
        * mri :           All the MRI data
            * anat :       All the anatomical MRI data
                * T1w :     T1-weighted MRI scans
                * T2w :     T2-weighted MRI scans
                * TSE :     Turbo Spin Echo MRI scans
                * FLAIR :   Fluid-inversion Recovery MRI scans
                * T2star :  T2-star quantitative scans
                * angio :   MR angiography scans
                * swi :     All susceptibility-weighted MRI data
            * func :       All fthe functional MRI data
                * pasl :    Pulsed arterial spin labeling
                * asl :     Arterial spin labelling
                * bold :    Blood-oxygenation level dependant (fMRI) scans
            * fmap :       All field maps
            * dwi :        All diffusion-weighted MRI data
        * pet :           All the PET data
            * fdg :        Fludeoxyglucose
            * pib :        Pittsburgh Compound B (amyloid)
            * av45 :       18F Florpiramine (tau)
            * av1451 :     18F Flortaucipir (tau)
        * ct              All the CT data
    * derivatives :      All derivatives
        * fs :            Freesurfer derivatives
        * fs-all :        Freesurfer derivatives (even non bidsifiable ones)
        * pup :           PET derivatives
    * meta :             All metadata
        * pheno :         Phenotypes

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `OASIS-2/sourcedata` folder must
        exist.
    keys : [list of] str
        Only bidsify these keys (all if empty)
    exclude_keys : [list of] str
        Do not bidsify these keys
    subs : [list of] int
        Only bidsify these subjects (all if empty)
    exclude_subs : [list of] int
        Do not bidsify these subjects
    json : {"yes", "no", "only"} | bool
        Whether to write (only) sidecar JSON files
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour when a file already exists
    log : str
        Path to log file
    """
    setup_filelog(log)

    # Format keys
    if isinstance(keys, str):
        keys = [keys]
    keys = set(keys or allleaves)

    if isinstance(exclude_keys, str):
        exclude_keys = [exclude_keys]
    exclude_keys = set(exclude_keys)

    # Format subjects
    if isinstance(subs, int):
        subs = [subs]
    subs = list(subs or [])

    if isinstance(exclude_subs, int):
        exclude_subs = [exclude_subs]
    exclude_subs = list(exclude_subs or [])

    # Format json
    if isinstance(json, bool):
        json = 'yes' if json else 'no'
    json = json.lower()

    # Get root
    root = get_tree_path(path) / DATASET

    # Bidsify
    Bidsifier(
        root,
        keys=keys,
        exclude_keys=exclude_keys,
        subs=subs,
        exclude_subs=exclude_subs,
        json=json,
        ifexists=if_exists,
    ).run()
