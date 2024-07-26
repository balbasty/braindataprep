"""
Expected input
--------------
label/
    {l|r}h.aparc.a200{5|9}s.annot
    {l|r}h.aparc.annot
    {l|r}h.cortex.label
mri/
    aparc+aseg.mgz
    aseg.mgz
    norm.mgz
    orig.mgz
    rawavg.mgz
surf/
    {l|r}h.area
    {l|r}h.area.pial
    {l|r}h.curv
    {l|r}h.defects
    {l|r}h.inflated
    {l|r}h.pial
    {l|r}h.smoothwm
    {l|r}h.sphere
    {l|r}h.sulc
    {l|r}h.thickness
    {l|r}h.white


Expected output
---------------
anat/
    # processed scans
    sub-{03d}_desc-orig_T1w.nii.gz                   [<-rawavg.mgz]
    sub-{03d}_res-1mm_desc-orig_T1w.nii.gz           [<-orig.mgz]
    sub-{03d}_res-1mm_desc-norm_T1w.nii.gz           [<-norm.mgz]
    # volume segmentations
    sub-{03d}_atlas-Aseg_dseg.nii.gz                 [<-aseg.mgz]
    sub-{03d}_atlas-AsegDesikanKilliany_dseg.nii.gz  [<-aparc+aseg.mgz]
    # surfaces
    sub-{03d}_hemi-{L|R}_pial.surf.gii               [<-{l|r}h.pial]
    sub-{03d}_hemi-{L|R}_wm.surf.gii                 [<-{l|r}h.white]
    sub-{03d}_hemi-{L|R}_smoothwm.surf.gii           [<-{l|r}h.smoothwm]
    sub-{03d}_hemi-{L|R}_inflated.surf.gii           [<-{l|r}h.inflated]
    sub-{03d}_hemi-{L|R}_sphere.surf.gii             [<-{l|r}h.sphere]
    # surface scalars
    sub-{03d}_hemi-{L|R}_curv.shape.gii              [<-{l|r}h.curv]
    sub-{03d}_hemi-{L|R}_thickness.shape.gii         [<-{l|r}h.thickness]
    sub-{03d}_hemi-{L|R}_sulc.shape.gii              [<-{l|r}h.sulc]
    sub-{03d}_hemi-{L|R}_defects.shape.gii           [<-{l|r}h.defects]
    sub-{03d}_hemi-{L|R}_desc-wm_area.shape.gii      [<-{l|r}h.area]
    sub-{03d}_hemi-{L|R}_desc-pial_area.shape.gii    [<-{l|r}h.area.pial]
    # surface segmentations
    sub-{03d}_hemi-{L|R}_atlas-DesikanKilliany_dseg.label.gii   [<-{l|r}h.aparc.annot]
    sub-{03d}_hemi-{L|R}_atlas-Destrieux_dseg.label.gii         [<-{l|r}h.aparc.a2009s.annot]

"""  # noqa: E501
from logging import getLogger
from functools import partial
from typing import Literal, Iterable
from pathlib import Path

from braindataprep.actions import Action
from braindataprep.actions import WriteJSON
from braindataprep.actions import BabelConvert
from braindataprep.actions import Freesurfer2Gifti
from braindataprep.freesurfer.lookup import write_lookup

lg = getLogger(__name__)


bidsifiable_vol_outputs = (
    'mri/rawavg.mgz',
    'mri/orig.mgz',
    'mri/norm.mgz',
    'mri/aseg.mgz',
    'mri/aparc+aseg.mgz',
)

bidsifiable_surf_outputs = (
    'surf/{hemi}h.pial',
    'surf/{hemi}h.white',
    'surf/{hemi}h.smoothwm',
    'surf/{hemi}h.inflated',
    'surf/{hemi}h.sphere',
    'surf/{hemi}h.curv',
    'surf/{hemi}h.thickness',
    'surf/{hemi}h.sulc',
    'surf/{hemi}h.defects',
    'surf/{hemi}h.area',
    'surf/{hemi}h.area.pial',
    'label/{hemi}h.aparc.annot',
    'label/{hemi}h.aparca2005s.annot',
    'label/{hemi}h.aparca2009s.annot',
)

bidsifiable_outputs = (
    bidsifiable_vol_outputs +
    tuple(map(lambda x: x.format(hemi='l'), bidsifiable_surf_outputs)) +
    tuple(map(lambda x: x.format(hemi='r'), bidsifiable_surf_outputs))
)


def bidsify_toplevel(
        dst: str | Path,
        fs_version: tuple = ()
) -> Iterable[Action]:
    """
    Yield actions that write toplevel TSV files, which describe
    FreeSurfer segmentations

    * atlas-Aseg_dseg.tsv
    * atlas-AsegDesikanKillian_dseg.tsv
    * atlas-Desikan-Killian_dseg.tsv
    * atlas-Destrieux_dseg.tsv

    Parameters
    ----------
    dst : str | Path
        Path to the freesurfer derivatives directory of the BIDS dataset
    fs_version : (int, int)
        Version of freesurfer used (MAJOR, MINOR)

    Yields
    ------
    Action
    """
    dst = Path(dst)

    yield Action(
        [], dst / 'atlas-Aseg_dseg.tsv',
        partial(write_lookup, mode='aseg'),
        mode="t", input="path",
    )

    yield Action(
        [], dst / 'atlas-AsegDesikanKillian_dseg.tsv',
        partial(write_lookup, mode='aparc+aseg'),
        mode="t", input="path",
    )

    yield Action(
        [], dst / 'atlas-Desikan-Killian_dseg.tsv',
        partial(write_lookup, mode='ak'),
        mode="t", input="path",
    )

    destrieux_mode = '2005' if fs_version < (4, 5) else '2009'
    yield Action(
        [], dst / 'atlas-Destrieux_dseg.tsv',
        partial(write_lookup, mode=destrieux_mode),
        mode="t", input="path",
    )


def bidsify(
        src: str | Path,
        dst: str | Path,
        source_t1: str | Iterable[str] | None = None,
        json: Literal['yes', 'no', 'only'] | bool = False
) -> Iterable[Action]:
    """
    Yield actions that bidsify a single Freesurfer subject

    Parameters
    ----------
    src : str | Path
        Path to the (input) Freesurfer subject
    dst : str | Path
        Path to the (output) BIDS derivative subject
        (".../derivatives/freesurfer-{MAJOR}.{minor}/sub-{d}")
    source_t1 : [list of] str | None
        Path to raw T1w data that was used as input to FreeSurfer
    json : bool or {'yes', 'no, 'only'}

    Yields
    ------
    Action
    """
    # --- init ---------------------------------------------------------
    json_mode = (
        'yes' if json is True else
        'no' if json is False else
        json
    ).lower()

    # Ensure Path
    if isinstance(source_t1, (str, Path)):
        source_t1 = [source_t1]
    source_t1 = list(map(str, source_t1))

    # Folders
    src = Path(src)
    dst = Path(dst)
    mri = src / 'mri'
    surf = src / 'surf'
    label = src / 'label'
    anat = dst / 'anat'

    # Subject name
    sub = dst.name
    if sub.startswith('ses'):
        sub = dst.parent.name + '_' + sub

    # --- helpers ------------------------------------------------------
    def make_base(convert: Action, pathinp: Path, pathout: Path, json: dict):
        if not pathinp.exists():
            return
        if json_mode != 'only':
            lg.info(f'write {pathout.name}')
            yield convert(pathinp, pathout)
        if json_mode != 'no':
            # need to get rid of two suffixes...
            pathjsn = pathout.with_name(pathout.stem).with_suffix('.json')
            lg.info(f'write {pathjsn.name}')
            yield WriteJSON(json, pathjsn)

    def make_nii(pathmgz: Path, pathnii: Path, json: dict):
        yield from make_base(BabelConvert, pathmgz, pathnii, json)

    def make_gii(pathfs: Path, pathgii: Path, json: dict):
        yield from make_base(Freesurfer2Gifti, pathfs, pathgii, json)

    # --- average in native space --------------------------------------
    # this is specific to OASIS (I think)
    res = ''
    if (mri / 'rawavg.mgz').exists():
        res = '_res-1mm'

    yield from make_nii(
        mri / 'rawavg.mgz',
        anat / f'{sub}_desc-orig_T1w.nii.gz',
        {
            "Description":
                "A T1w scan, averaged across repeats",
            "SkullStripped":
                False,
            "Resolution":
                "Native resolution",
            "Sources":
                source_t1,
        }
    )

    # === mri ==========================================================
    # --- average in native space --------------------------------------
    yield from make_nii(
        mri / 'orig.mgz',
        anat / f'{sub}{res}_desc-orig_T1w.nii.gz',
        {
            "Description":
                "A T1w scan, resampled to 1mm isotropic",
            "SkullStripped":
                False,
            "Resolution":
                "1mm isotropic",
            "Sources": (
                [f'bids::{sub}/anat/{sub}_desc-orig_T1w.nii.gz']
                if res else source_t1
            ),
        }
    )
    # --- normalized image ---------------------------------------------
    yield from make_nii(
        mri / 'norm.mgz',
        anat / f'{sub}{res}_desc-norm_T1w.nii.gz',
        {
            "Description":
                "A T1w scan, skull-stripped and intensity-normalized",
            "SkullStripped":
                True,
            "Resolution":
                "1mm isotropic",
            "Sources": [
                f'bids::{sub}/anat/{sub}_desc-orig_T1w.nii.gz'
            ]
        }
    )
    # === label ========================================================
    # --- aseg ---------------------------------------------------------
    yield from make_nii(
        mri / 'aseg.mgz',
        anat / f'{sub}_atlas-Aseg_dseg.nii.gz',
        {
            "Description":
                "A segmentation of the T1w scan into cortex, white "
                "matter, and subcortical structures",
            "Sources": [
                f'bids::{sub}/anat/{sub}{res}_desc-norm_T1w.nii.gz'
            ]
        }
    )
    # --- aparc+aseg ---------------------------------------------------
    yield from make_nii(
        mri / 'aparc+aseg.mgz',
        anat / f'{sub}_atlas-AsegDesikanKilliany_dseg.nii.gz',
        {
            "Description":
                "A segmentation of the T1w scan into cortical parcels, "
                "white matter, and subcortical structures",
            "Sources": [
                f'bids::{sub}/anat/{sub}_atlas-Aseg_dseg.nii.gz',
                f'bids::{sub}/anat/{sub}_hemi-L_atlas-DesikanKilliany_dseg.label.gii',  # noqa: E501
                f'bids::{sub}/anat/{sub}_hemi-R_atlas-DesikanKilliany_dseg.label.gii',  # noqa: E501
            ]
        },
    )
    for hemi in ('L', 'R'):
        # === surf =====================================================
        # --- wm -------------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.white',
            anat / f'{sub}_hemi-{hemi}_wm.surf.gii',
            {
                "Description":
                    "White matter surface",
                "Sources": [
                    f'bids::{sub}/anat/{sub}{res}_desc-norm_T1w.nii.gz',
                    f'bids::{sub}/anat/{sub}_atlas-Aseg_dseg.nii.gz',
                ]
            }
        )
        # --- pial -----------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.pial',
            anat / f'{sub}_hemi-{hemi}_pial.surf.gii',
            {
                "Description":
                    "Pial surface",
                "Sources": [
                    f'bids::{sub}/anat/{sub}{res}_desc-norm_T1w.nii.gz',
                    f'bids::{sub}/anat/{sub}_atlas-Aseg_dseg.nii.gz',
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }
        )
        # --- smoothwm -------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.smoothwm',
            anat / f'{sub}_hemi-{hemi}_smoothwm.surf.gii',
            {
                "Description":
                    "Smoothed white matter surface",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }
        )
        # --- inflated -------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.inflated',
            anat / f'{sub}_hemi-{hemi}_inflated.surf.gii',
            {
                "Description":
                    "Inflated white matter surface",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }
        )
        # --- sphere ---------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.sphere',
            anat / f'{sub}_hemi-{hemi}_sphere.surf.gii',
            {
                "Description":
                    "White matter surface mapped to a sphere",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }
        )
        # === surf : scalars ===========================================
        # --- curv -----------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.curv',
            anat / f'{sub}_hemi-{hemi}_curv.shape.gii',
            {
                "Description":
                    "Smoothed mean curvature of the white matter "
                    "surface (Fischl et al., 1999)",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }
        )
        # --- sulc -----------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.sulc',
            anat / f'{sub}_hemi-{hemi}_sulc.shape.gii',
            {
                "Description":
                    "Smoothed average convexity of the white matter "
                    "surface (Fischl et al., 1999)",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }
        )
        # --- thickness ------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.thickness',
            anat / f'{sub}_hemi-{hemi}_thickness.shape.gii',
            {
                "Description":
                    "Cortical thickness (distance from each white matter "
                    "vertex to its nearest point on the pial surface)",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_pial.surf.gii',
                ]
            }
        )
        # --- wm.area --------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.area',
            anat / f'{sub}_hemi-{hemi}_desc-wm_area.shape.gii',
            {
                "Description":
                    "Discretized white matter surface area across regions",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }
        )
        # --- pial.area ------------------------------------------------
        yield from make_gii(
            surf / f'{hemi.lower()}h.area.pial',
            anat / f'{sub}_hemi-{hemi}_desc-pial_area.shape.gii',
            {
                "Description":
                    "Discretized pial surface area across regions",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_pial.surf.gii',
                ]
            }
        )
        # === surf : labels ============================================
        # --- DK -------------------------------------------------------
        yield from make_gii(
            label / f'{hemi.lower()}h.aparc.annot',
            anat / f'{sub}_hemi-{hemi}_atlas-DesikanKilliany_dseg.label.gii',
            {
                "Description":
                    "Cortical parcellation based on the Desikan-Killiany "
                    "atlas",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_smoothwm.surf.gii',
                ]
            }
        )
        # --- Destrieux2005 --------------------------------------------
        yield from make_gii(
            label / f'{hemi.lower()}h.a2005s.annot',
            anat / f'{sub}_hemi-{hemi}_atlas-Destrieux_dseg.label.gii',
            {
                "Description":
                    "Cortical parcellation based on the Destrieux (2005) "
                    "atlas",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_smoothwm.surf.gii',
                ]
            }
        )
        # --- Destrieux2009 --------------------------------------------
        yield from make_gii(
            label / f'{hemi.lower()}h.a2009s.annot',
            anat / f'{sub}_hemi-{hemi}_atlas-Destrieux_dseg.label.gii',
            {
                "Description":
                    "Cortical parcellation based on the Destrieux (2009) "
                    "atlas",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_smoothwm.surf.gii',
                ]
            }
        )
