"""
Expected input
--------------
OASIS-1/
    sourcedata/
        oasis_cross-sectional_disc{1..12}.tar.gz
        oasis_cs_freesurfer_disc{1..12}.tar.gz
        oasis_cross-sectional.csv
        oasis_cross-sectional-reliability.csv
        oasis_cross-sectional_facts.pdf

Expected output
---------------
OASIS-1/
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
from pathlib import Path
from typing import Literal, Iterable

from braindataprep.utils.log import setup_filelog
from braindataprep.utils.path import get_tree_path
from braindataprep.actions.action import IfExists
from braindataprep.datasets.OASIS.I.command import oasis1
from braindataprep.datasets.OASIS.I.bidsifier import Bidsifier

KeyChoice = Literal[
    "meta", "raw", "avg", "tal", "fsl", "fs", "fs-all"
]


@oasis1.command
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.__args__,
    discs: Iterable[int] | None = tuple(range(1, 13)),
    subs: Iterable[int] | None = tuple(),
    exclude_subs: Iterable[int] | None = tuple(),
    json: Literal["yes", "no", "only"] | bool = "yes",
    if_exists: IfExists.Choice = "skip",
    log: str | None = None,
):
    """
    Convert source data into a BIDS-compliant directory

    Parameters
    ----------
    path : str
        Path to root of all datasets. An `OASIS-1/sourcedata` folder must
        exist.
    keys : [list of] {"meta", "raw", "avg", "tal", "fsl", "fs", "fs-all"}
        Only bidsify these keys
    discs : [list of] {1..12}
        Discs to bidsify.
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
    keys = set(keys or KeyChoice.__args__)

    # Format discs
    if isinstance(discs, int):
        discs = [discs]
    discs = list(discs or [])

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
    root = get_tree_path(path) / 'OASIS-1'

    # Bidsify
    Bidsifier(
        root,
        keys=keys,
        discs=discs,
        subs=subs,
        exclude_subs=exclude_subs,
        json=json,
        ifexists=if_exists,
    ).run()
