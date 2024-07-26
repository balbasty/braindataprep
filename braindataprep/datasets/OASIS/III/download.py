from pathlib import Path
from typing import Iterable
from humanize import naturalsize

from braindataprep.utils.ui import human2bytes
from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.download import DownloadManager
from braindataprep.download import IfExists
from braindataprep.download import CHUNK_SIZE
from braindataprep.xnat import XNAT
from braindataprep.datasets.OASIS.III.command import oasis3
from braindataprep.datasets.OASIS.III.keys import allkeys
from braindataprep.datasets.OASIS.III.keys import flatten_keys
from braindataprep.datasets.OASIS.III.keys import compat_keys

from logging import getLogger
lg = getLogger(__name__)


@oasis3.command
def download(
    path: str | None = None,
    *,
    keys: Iterable[str] = tuple(),
    subs: Iterable[int] | None = tuple(),
    exclude_subs: Iterable[int] | None = tuple(),
    if_exists: IfExists.Choice = "skip",
    user: str | None = None,
    password: str | None = None,
    packet: int | str = naturalsize(CHUNK_SIZE),
    log: str | None = None,
):
    """
    Download source data for the OASIS-III dataset.

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
        * pup :           PET derivatives
    * meta :             All metadata
        * pheno :         Phenotypes


    Parameters
    ----------
    path : str
        Path to root of all datasets. An `OASIS-2` folder will be created.
    keys : [list of] str
        Data categories to download
    subs : [list of] int
        Only bidsify these subjects (all if empty)
    exclude_subs : [list of] int
        Do not bidsify these subjects
    if_exists : {"error", "skip", "overwrite", "different", "refresh"}
        Behaviour if a file already exists
    user : str
        NITRC username
    password : str
        NITRC password
    packet : int
        Packet size to download, in bytes
    log : str
        Path to log file

    """
    setup_filelog(log)
    path = Path(get_tree_path(path))
    keys = set(keys or flatten_keys(allkeys))
    src = path / 'OASIS-3' / 'sourcedata'

    xnat = XNAT(user, password, open=True)

    # Format subjects
    if isinstance(subs, (int, str)):
        subs = [subs]
    subs = list(subs or [])

    if isinstance(exclude_subs, int):
        exclude_subs = [exclude_subs]
    exclude_subs = set(exclude_subs or [])

    # Get subject IDs
    if not subs:
        subs = xnat.get_subjects('OASIS3')
        subs = [int(sub[4:]) for sub in subs if sub.startswith('OAS3')]
    elif subs and isinstance(subs[0], str):
        # Might be a file that contains subject IDs
        tmp, subs = subs, []
        for sub in tmp:
            if Path(sub).exists():
                with open(sub, 'rt') as f:
                    for line in f:
                        subs.append(int(line))
            else:
                subs.append(int(sub))
    subs = set(subs) - exclude_subs

    # Accumulate downloaders
    def all_downloaders():
        opt = dict(chunk_size=human2bytes(packet), ifexists=if_exists)

        # Get downloaders for metadata
        if (keys & compat_keys("meta")):
            yield from xnat.get_all_downloaders(
                'OASIS3', '0AS_data_files', 'OASIS3_data_files', dst=src, **opt
            )

        # Get downloaders for image data
        for sub in subs:
            experiments = xnat.get_experiments('OASIS3', f'OAS3{sub:04d}')
            for experiment in experiments:

                # early filter on experiment type
                experiment_type = experiment.split('_')[1]
                if experiment_type == 'MR':
                    if not (keys & compat_keys("mri")):
                        continue
                elif experiment_type == "CT":
                    if not (keys & compat_keys("ct")):
                        continue
                elif experiment_type == "FDG":
                    if not (keys & compat_keys("fdg")):
                        continue
                elif experiment_type == "PIB":
                    if not (keys & compat_keys("pib")):
                        continue
                elif experiment_type == "AV45":
                    if not (keys & compat_keys("av45")):
                        continue
                else:
                    continue

                scans = xnat.get_scans('OASIS3', f'OAS3{sub:04d}', experiment,
                                       return_info=True)
                for scan in scans:
                    # filter on scan type (maybe not robust enough?)
                    keep_scan = bool(keys & compat_keys(scan['type']))
                    if not keep_scan:
                        continue
                    scan = scan['ID']
                    fname = src / experiment / f'{scan}.tar.gz'
                    yield xnat.get_downloader(
                        'OASIS3', f'OAS3{sub:04d}', experiment, scan, fname,
                        **opt
                    )

                # derivatives

                if keys & compat_keys("fs"):
                    assessors = xnat.get_all_assessors(
                        'OASIS3', f'OAS3{sub:04d}', experiment, '*Freesurfer*'
                    )
                    for assessor in assessors:
                        assessor = assessor.split('/')[-1]
                        fname = src / experiment / f'{assessor}.tar.gz'

                        yield xnat.get_downloader(
                            'OASIS3', f'OAS3{sub:04d}', experiment, assessor,
                            fname, type='assessor', **opt
                        )

                if keys & compat_keys("pup"):
                    assessors = xnat.get_all_assessors(
                        'OASIS3', f'OAS3{sub:04d}', experiment,
                        '*PUPTIMECOURSE*'
                    )
                    for assessor in assessors:
                        assessor = assessor.split('/')[-1]
                        fname = src / experiment / f'{assessor}.tar.gz'

                        yield xnat.get_downloader(
                            'OASIS3', f'OAS3{sub:04d}', experiment, assessor,
                            fname, type='assessor', **opt
                        )

    # Download all
    DownloadManager(all_downloaders(), ifexists=if_exists, path='full').run()
    xnat.close()
