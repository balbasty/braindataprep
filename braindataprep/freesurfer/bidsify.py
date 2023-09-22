import os
import logging
from braindataprep.utils import nibabel_convert, write_json
from braindataprep.freesurfer.lookup import write_lookup
from braindataprep.freesurfer.io import nibabel_fs2gii

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


def bidsify_toplevel(dst, fs_version=()):
    logging.info('write atlas-Aseg_dseg.tsv')
    write_lookup(
        os.path.join(dst, 'atlas-Aseg_dseg.tsv'),
        'aseg'
    )
    logging.info('write atlas-AsegDesikanKillian_dseg.tsv')
    write_lookup(
        os.path.join(dst, 'atlas-AsegDesikanKillian_dseg.tsv'),
        'aparc+aseg'
    )
    logging.info('write atlas-Desikan-Killian_dseg.tsv')
    write_lookup(
        os.path.join(dst, 'atlas-Desikan-Killian_dseg.tsv'),
        'dk'
    )
    logging.info('write atlas-Destrieux_dseg.tsv')
    write_lookup(
        os.path.join(dst, 'atlas-Destrieux_dseg.tsv'),
        '2005' if fs_version < (4, 5) else '2009'
    )


def bidsify(src, dst, source_t1=None, json_only=False):
    """
    Bidsify a single Freesurfer subject

    Parameters
    ----------
    src : str
        Path to the (input) Freesurfer subject
    dst : str
        Path to the (output) BIDS derivative subject
        (".../derivatives/freesurfer-{MAJOR}.{minor}/sub-{d}")
    """
    if source_t1:
        if isinstance(source_t1, str):
            source_t1 = [source_t1]
        else:
            source_t1 = list(source_t1)
    else:
        source_t1 = []

    sub = os.path.basename(dst)
    os.makedirs(os.path.join(dst, 'anat'), exist_ok=True)

    # --- average in native space --------------------------------------
    # this is specific to OASIS (I think)
    resflag = ''
    if os.path.exists(os.path.join(src, 'mri', 'rawavg.mgz')):
        basename = f'{sub}_desc-orig_T1w'
        logging.info(f'write {basename}.nii.gz')
        if not json_only:
            nibabel_convert(
                os.path.join(src, 'mri', 'rawavg.mgz'),
                os.path.join(dst, 'anat', f'{basename}.nii.gz'),
            )
        write_json({
            "Description": "A T1w scan, averaged across repeats",
            "SkullStripped": False,
            "Resolution": "Native resolution",
            "Sources": source_t1,
        }, os.path.join(dst, 'anat', f'{basename}.json'))
        resflag = '_res-1mm'

    # === mri ==========================================================
    # --- average in native space --------------------------------------
    if os.path.exists(os.path.join(src, 'mri', 'orig.mgz')):
        basename = f'{sub}{resflag}_desc-orig_T1w'
        logging.info(f'write {basename}.nii.gz')
        if not json_only:
            nibabel_convert(
                os.path.join(src, 'mri', 'orig.mgz'),
                os.path.join(dst, 'anat', f'{basename}.nii.gz'),
            )
        write_json({
            "Description": "A T1w scan, resampled to 1mm isotropic",
            "SkullStripped": False,
            "Resolution": "1mm isotropic",
            "Sources": (
                [f'bids::{sub}/anat/{sub}_desc-orig_T1w.nii.gz']
                if resflag else source_t1
            ),
        }, os.path.join(dst, 'anat', f'{basename}.json'))
    # --- normalized image ---------------------------------------------
    if os.path.exists(os.path.join(src, 'mri', 'norm.mgz')):
        basename = f'{sub}{resflag}_desc-norm_T1w'
        logging.info(f'write {basename}.nii.gz')
        if not json_only:
            nibabel_convert(
                os.path.join(src, 'mri', 'norm.mgz'),
                os.path.join(dst, 'anat', f'{basename}.nii.gz'),
            )
        write_json({
            "Description": ("A T1w scan, skull-stripped and "
                            "intensity-normalized"),
            "SkullStripped": True,
            "Resolution": "1mm isotropic",
            "Sources": [
                f'bids::{sub}/anat/{sub}_desc-orig_T1w.nii.gz'
            ]
        }, os.path.join(dst, 'anat', f'{basename}.json'))
    # === label ========================================================
    # --- aseg ---------------------------------------------------------
    if os.path.exists(os.path.join(src, 'mri', 'aseg.mgz')):
        basename = f'{sub}_atlas-Aseg_dseg'
        logging.info(f'write {basename}.nii.gz')
        if not json_only:
            nibabel_convert(
                os.path.join(src, 'mri', 'aseg.mgz'),
                os.path.join(dst, 'anat', f'{basename}.nii.gz'),
            )
        write_json({
            "Description": ("A segmentation of the T1w scan into "
                            "cortex, white matter, and subcortical "
                            "structures"),
            "Sources": [
                f'bids::{sub}/anat/{sub}{resflag}_desc-norm_T1w.nii.gz'
            ]
        }, os.path.join(dst, 'anat', f'{basename}.json'))
    # --- aparc+aseg ---------------------------------------------------
    if os.path.exists(os.path.join(src, 'mri', 'aparc+aseg.mgz')):
        basename = f'{sub}_atlas-AsegDesikanKilliany_dseg'
        logging.info(f'write {basename}.nii.gz')
        if not json_only:
            nibabel_convert(
                os.path.join(src, 'mri', 'aparc+aseg.mgz'),
                os.path.join(dst, 'anat', f'{basename}.nii.gz'),
            )
        write_json({
            "Description": ("A segmentation of the T1w scan into "
                            "cortical parcels, white matter, and "
                            "subcortical structures"),
            "Sources": [
                f'bids::{sub}/anat/{sub}_atlas-Aseg_dseg.nii.gz',
                f'bids::{sub}/anat/{sub}_hemi-L_atlas-DesikanKilliany_dseg.label.gii',  # noqa: E501
                f'bids::{sub}/anat/{sub}_hemi-R_atlas-DesikanKilliany_dseg.label.gii',  # noqa: E501
            ]
        }, os.path.join(dst, 'anat', f'{basename}.json'))
    for hemi in ('L', 'R'):
        # === surf =====================================================
        # --- wm -------------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.white')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_wm'
            logging.info(f'write {basename}.surf.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.surf.gii'),
                )
            write_json({
                "Description": "White matter surface",
                "Sources": [
                    f'bids::{sub}/anat/{sub}{resflag}_desc-norm_T1w.nii.gz',
                    f'bids::{sub}/anat/{sub}_atlas-Aseg_dseg.nii.gz',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- pial -----------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.pial')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_pial'
            logging.info(f'write {basename}.surf.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.surf.gii'),
                )
            write_json({
                "Description": "Pial surface",
                "Sources": [
                    f'bids::{sub}/anat/{sub}{resflag}_desc-norm_T1w.nii.gz',
                    f'bids::{sub}/anat/{sub}_atlas-Aseg_dseg.nii.gz',
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- smoothwm -------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.smoothwm')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_smoothwm'
            logging.info(f'write {basename}.surf.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.surf.gii'),
                )
            write_json({
                "Description": "Smoothed white matter surface",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- inflated -------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.inflated')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_inflated'
            logging.info(f'write {basename}.surf.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.surf.gii'),
                )
            write_json({
                "Description": "Inflated white matter surface",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- sphere ---------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.sphere')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_sphere'
            logging.info(f'write {basename}.surf.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.surf.gii'),
                )
            write_json({
                "Description": "White matter surface mapped to a sphere",
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # === surf : scalars ===========================================
        # --- curv -----------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.curv')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_curv'
            logging.info(f'write {basename}.shape.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.shape.gii'),
                )
            write_json({
                "Description": ("Smoothed mean curvature of the white "
                                "matter surface (Fischl et al., 1999)"),
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- sulc -----------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.sulc')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_sulc'
            logging.info(f'write {basename}.shape.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.shape.gii'),
                )
            write_json({
                "Description": ("Smoothed average convexity of the white "
                                "matter surface (Fischl et al., 1999)"),
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- thickness ------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.thickness')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_thickness'
            logging.info(f'write {basename}.shape.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.shape.gii'),
                )
            write_json({
                "Description": ("Cortical thickness (distance from "
                                "each white matter vertex to its nearest "
                                "point on the pial surface)"),
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_pial.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- wm.area --------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.area')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_desc-wm_area'
            logging.info(f'write {basename}.shape.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.shape.gii'),
                )
            write_json({
                "Description": ("Discretized surface area across regions"),
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_wm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- pial.area ------------------------------------------------
        inpname = os.path.join(src, 'surf', f'{hemi.lower()}h.area.pial')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_desc-pial_area'
            logging.info(f'write {basename}.shape.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.shape.gii'),
                )
            write_json({
                "Description": ("Discretized surface area across regions"),
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_pial.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # === surf : labels ============================================
        # --- DK -------------------------------------------------------
        inpname = os.path.join(src, 'label', f'{hemi.lower()}h.aparc.annot')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_atlas-DesikanKilliany_dseg'
            logging.info(f'write {basename}.label.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.label.gii'),
                )
            write_json({
                "Description": ("Cortical parcellation based on the "
                                "Desikan-Killiany atlas"),
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_smoothwm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- Destrieux2005 --------------------------------------------
        inpname = os.path.join(src, 'label', f'{hemi.lower()}h.a2005s.annot')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_atlas-Destrieux_dseg'
            logging.info(f'write {basename}.label.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.label.gii'),
                )
            write_json({
                "Description": ("Cortical parcellation based on the "
                                "Destrieux (2005) atlas"),
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_smoothwm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
        # --- Destrieux2009 --------------------------------------------
        inpname = os.path.join(src, 'label', f'{hemi.lower()}h.a2009s.annot')
        if os.path.exists(inpname):
            basename = f'{sub}_hemi-{hemi}_atlas-Destrieux_dseg'
            logging.info(f'write {basename}.label.gii')
            if not json_only:
                nibabel_fs2gii(
                    inpname,
                    os.path.join(dst, 'anat', f'{basename}.label.gii'),
                )
            write_json({
                "Description": ("Cortical parcellation based on the "
                                "Destrieux (2009) atlas"),
                "Sources": [
                    f'bids::{sub}/anat/{sub}_hemi-{hemi}_smoothwm.surf.gii',
                ]
            }, os.path.join(dst, 'anat', f'{basename}.json'))
