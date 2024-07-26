import tarfile
import nibabel as nib
import numpy as np
from logging import getLogger
from functools import partial
from gzip import GzipFile
from pathlib import Path, PosixPath
from typing import Literal, Iterable, Set

from braindataprep.utils.path import fileparts
from braindataprep.utils.io import copy_from_buffer
from braindataprep.utils.io import write_tsv
from braindataprep.pyout import bidsify_tab
from braindataprep.actions import Action
from braindataprep.actions import File
from braindataprep.actions import CopyBytes
from braindataprep.actions import CopyJSON
from braindataprep.actions import WrapAction

lg = getLogger(__name__)
try:
    import xlrd
except ImportError:
    lg.error(
        'Cannot find `xlrd`. Did you install with [ixi] flag? '
        'Try `pip install braindataprep[ixi]`.'
    )

KeyChoice = Literal["meta", "T1w", "T2w", "PDw", "angio", "dwi"]
IfExists = Literal["error", "skip", "overwrite", "different", "refresh"]


class Bidsifier:

    # Folder containing template README/JSON/...
    TPLDIR = Path(__file__).parent / 'templates'

    # Conversion between IXI and BIDS modality names
    IXI2BIDS = {
        'T1': 'T1w',
        'T2': 'T2w',
        'PD': 'PDw',
        'MRA': 'angio',
        'DTI': 'dwi',
    }
    BIDS2IXI = {
        'T1w': 'T1',
        'T2w': 'T2',
        'PDw': 'PD',
        'angio': 'MRA',
        'dwi': 'DTI',
    }

    def __init__(
        self,
        root: Path,
        *,
        keys: Set[KeyChoice] = set(KeyChoice.__args__),
        subs: Iterable[int] = tuple(),
        exclude_subs: Iterable[int] = tuple(),
        json: Literal["yes", "no", "only"] | bool = True,
        ifexists: IfExists = "skip"
    ):
        self.root = root
        self.keys = keys
        self.subs = subs
        self.exclude_subs = exclude_subs
        self.json = json
        self.ifexists = ifexists

    def init(self):
        """Prepare common stuff"""
        # Printer
        self.out = bidsify_tab()
        # Folder
        self.src = self.root / 'sourcedata'
        self.raw = self.root / 'rawdata'
        # Track errors
        self.nb_errors = 0
        self.nb_skipped = 0

    # ------------------------------------------------------------------
    #   Run all
    # ------------------------------------------------------------------
    def run(self):
        """Run all actions"""
        self.init()
        with self.out as self.out:
            self._run()

    def _run(self):
        """Must be run from inside the `out` context."""
        # Metadata
        if 'meta' in self.keys:
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_meta():
                status.setdefault('modality', 'meta')
                self.out(status)

        # T1/T2/PD/MRA are simple "anat" scans that can be processed
        # identically.
        for key in self.keys.intersection(set(['T1w', 'T2w', 'PDw', 'angio'])):
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_modality(key):
                status.setdefault('modality', key)
                self.out(status)

        # DWI scans are stored as individual 3D niftis (one per bval/bvec)
        # whereas BIDS prefers 4D niftis
        # We also need to deal with the bvals/bvecs files.
        if 'dwi' in self.keys:
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_dwi():
                status.setdefault('modality', 'dwi')
                self.out(status)

    # ------------------------------------------------------------------
    #   Helpers
    # ------------------------------------------------------------------
    @staticmethod
    def get_sites(tarpath: Path) -> dict:
        """Get all available sites"""
        sitemap = {}
        with File(tarpath, "r") as f:
            with tarfile.open(str(f.safename)) as tar:
                for member in tar.getmembers():
                    ixi_id, site, *_ = member.name.split('-')
                    ixi_id = int(ixi_id[3:])
                    sitemap[ixi_id] = site
        return sitemap

    def fixstatus(self, status: dict, fname: str):
        status.setdefault('path', fname)
        yield status
        if status.get('status', '') == 'error':
            self.nb_errors += 1
            yield {'errors': self.nb_errors}
        elif status.get('status', '') == 'skipped':
            self.nb_skipped += 1
            yield {'skipped': self.nb_skipped}

    # ------------------------------------------------------------------
    #   Generate root metadata (dataset, participants, etc)
    # ------------------------------------------------------------------
    def make_meta(self):

        # The mapping from subject to site is only available through
        # individual filenames. We therefore need to first parse one of
        # the tar to build this mapping.
        # TODO: read-protect tar archive using `File`
        sites = None
        for key in ['T1', 'T2', 'PD', 'MRA', 'DTI']:
            tarpath = self.src / f'IXI-{key}.tar'
            if tarpath.exists():
                sites = self.get_sites(tarpath)
                break
        if sites is None:
            lg.error("No tar file available. Cannot compute sites.")

        # Define future actions
        opt = dict(ifexists=self.ifexists)
        actions = {
            'README':
                CopyBytes(
                    self.TPLDIR / 'README',
                    self.root / 'README',
                    **opt
                ),
            'dataset_description.json':
                CopyJSON(
                    self.TPLDIR / 'dataset_description.json',
                    self.root / 'dataset_description.json',
                    **opt
                ),
            'participants.json':
                CopyJSON(
                    self.TPLDIR / 'participants.json',
                    self.root / 'participants.json',
                    **opt
                ),
            'participants.tsv':
                WrapAction(
                    self.src / 'IXI.xls',
                    self.root / 'participants.tsv',
                    partial(self.make_participants, sites=sites),
                    mode="t", input="path",
                    **opt
                ),
        }

        # Perform actions
        yield {'progress': 0}
        for i, (fname, action) in enumerate(actions.items()):
            for status in action:
                yield from self.fixstatus(status, fname)
            yield {'progress': 100*(i+1)/len(actions)}
        yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Generate structural modalities
    # ------------------------------------------------------------------
    def make_modality(self, key: str):
        # Check that archive is available, and open it
        tarpath = self.src / f'IXI-{self.BIDS2IXI[key]}.tar'
        # if tar is None:
        if not tarpath.exists():
            lg.warning(f'IXI-{self.BIDS2IXI[key]}.tar not found')
            return
        with tarfile.open(tarpath) as tar:
            yield from self._make_modality(key, tar)

    def _make_modality(self, key: str, tar):
        tarpath = self.src / f'IXI-{self.BIDS2IXI[key]}.tar'
        opt = dict(ifexists=self.ifexists)

        def skip_subject(id):
            return ((self.subs and id not in self.subs)
                    or id in self.exclude_subs)

        def parse_member(member):

            # Get name
            path = PosixPath(member.name)
            id, site, *_ = path.name.split('-')
            id = int(id[3:])
            if skip_subject(id):
                return
            dst = self.raw / f'sub-{id:03d}' / 'anat'
            name = f'sub-{id:03d}_{key}'

            # JSON file
            if self.json != 'no':
                fname = name + '.json'
                for status in CopyJSON(
                    self.TPLDIR / site / f'{key}.json', dst / fname, **opt
                ):
                    yield from self.fixstatus(status, fname)

            # NIFTI file
            if self.json != 'only':
                fname = name + '.nii.gz'
                for status in Action(
                    tarpath, dst / fname,
                    lambda fp: copy_from_buffer(tar.extractfile(member), fp),
                    **opt
                ):
                    yield from self.fixstatus(status, fname)

        # Count number of subjects
        nsub = 0
        for path in tar.getnames():
            path = PosixPath(path)
            id = int(path.name.split('-')[0][3:])
            nsub += not skip_subject(id)

        # Process each subject
        isub = 0
        for member in tar.getmembers():
            path = PosixPath(member.name)
            id = int(path.name.split('-')[0][3:])
            if skip_subject(id):
                continue
            isub += 1
            yield from parse_member(member)
            yield {'progress': 100*isub/nsub}

        yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Generate DWI modality
    # ------------------------------------------------------------------
    def make_dwi(self):
        # Check that archive is available, and open it
        tarpath = self.src / 'IXI-DTI.tar'
        if not tarpath.exists():
            lg.warning('IXI-DTI.tar not found')
            return
        with tarfile.open(tarpath) as tar:
            yield from self._make_dwi(tar)

    def _make_dwi(self, tar):
        tarpath = self.src / 'IXI-DTI.tar'
        opt = dict(ifexists=self.ifexists)

        # First, copy bvals/bvecs.
        # They are common to all subjects so we place them at the top of
        # the tree (under "rawdata/")

        fname = 'dwi.bval'
        if not (self.src / 'bvals.txt').exists():
            lg.error('bvals not found')
            yield from self.fixstatus(
                {'status': 'error', 'message': 'file not found'}, fname
            )
        else:
            for status in CopyBytes(
                self.src / 'bvals.txt', self.raw / fname, **opt
            ):
                yield from self.fixstatus(status, fname)

        fname = 'dwi.bvec'
        if not (self.src / 'bvecs.txt').exists():
            lg.error('bvecs not found')
            yield from self.fixstatus(
                {'status': 'error', 'message': 'file not found'}, fname
            )
        else:
            for status in CopyBytes(
                self.src / 'bvecs.txt', self.raw / fname,  **opt
            ):
                yield from self.fixstatus(status, fname)

        # Process individual subjects
        # DWI scans are stored as individual 3D volumes in the archive,
        # but BIDS prefers 4D files. For each subject, we unpack each
        # 3D volume in memory, concatenate them and store them as a 4D
        # nifti.
        #
        # NOTE
        #   we concatenate along the 4-th dimension (indexed 3)
        #   nifti states that the 4-th dimension is reserved for time,
        #   but in terms of acquisition, a dwi series is really like an
        #   fmri time series, where gradients change at each volume.
        #   AY says directions are always along the 4-th dimension, not
        #   the 5-th, in the diffusion world).

        # Get list of all subjects and map to their channels and site
        ids = {}
        chs = {}
        sts = {}
        for path in tar.getnames():
            # Get ID
            _, basename, _ = fileparts(path)
            id, site, *_, dti_id = basename.split('-')
            id = int(id[3:])
            if (
                (self.subs and id not in self.subs) or
                id in self.exclude_subs
            ):
                continue
            sts[id] = site
            ids.setdefault(id, [])
            chs.setdefault(id, [])
            ids[id].append(path)
            chs[id].append(int(dti_id))

        # reorder channels
        for id in ids:
            tmp = list(zip(ids[id], chs[id]))
            tmp.sort(key=lambda x: x[1])
            ids[id] = [x[0] for x in tmp]
            chs[id] = [x[1] for x in tmp]
        nsub = len(ids)

        # Loop through each subject
        isub = 0
        for id, site in sts.items():
            isub += 1

            dst = self.raw / f'sub-{id:03d}' / 'dwi'
            basename = f'sub-{id:03d}_dwi'

            # Write JSON
            if self.json != 'no':
                name = basename + '.json'
                lg.info(f'write {name}')
                for status in CopyJSON(
                    self.TPLDIR / site / 'dwi.json', dst / name, **opt
                ):
                    yield from self.fixstatus(status, name)

            if self.json == 'only':
                yield {'progress': 100*isub/nsub}
                continue

            # Now, concatenate volumes

            # ----------------------------------------------------------
            # This is our (future) concatenatino action for delayed
            def cat_action(path):
                # Load all channels
                membernames = ids[id]
                dat = []
                for i, membername in enumerate(membernames):
                    yield {'status': f'load ch-{i:02d}'}
                    member = tar.getmember(membername)
                    nii = nib.Nifti1Image.from_stream(
                        GzipFile(fileobj=tar.extractfile(member))
                    )
                    dat.append(np.asarray(nii.dataobj).squeeze())

                # Fallback (this happened in one of the subjects...)
                if len(set([tuple(dat1.shape) for dat1 in dat])) > 1:
                    lg.error(
                        f'sub-{id:03d}_dwi | incompatible shapes'
                    )
                    raise RuntimeError('incompatible shapes')

                # Ensure 4D
                yield {'status': 'stacking channels'}
                dat = list(map(lambda x: x[..., None], dat))
                dat = np.concatenate(dat, axis=3)
                yield {'status': 'writing stack'}
                affine, header = nii.affine, nii.header
                nib.save(nib.Nifti1Image(dat, affine, header), path)
            # ----------------------------------------------------------

            name = basename + '.nii.gz'
            for status in Action(
                tarpath, dst / name, cat_action,
                ifexists=self.ifexists, input='path',
            ):
                yield from self.fixstatus(status, name)

            yield {'progress': 100*isub/nsub}

        yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Generate participant file
    # ------------------------------------------------------------------
    @staticmethod
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

        def parse_date(date):
            if not isinstance(date, str):
                date = xlrd.xldate.xldate_as_tuple(date, book.datemode)
                date = f'{date[0]:04d}-{date[1]:02d}-{date[2]:02d}'
            return date

        def iter_rows():
            yield participants_header
            for n in range(1, sheet.nrows):
                ixi_row = sheet.row(n)
                if ixi_row[ixi_header.index("DATE_AVAILABLE")].value == 0:
                    continue
                ixi_id = int(ixi_row[ixi_header.index("IXI_ID")].value)
                if ixi_id not in sites:
                    continue
                participant = [
                    f'sub-{ixi_id:03d}',
                    sites[ixi_id],
                    ixi_row[ixi_header.index('AGE')].value or 'n/a',
                    ixi_age.get(
                        ixi_row[ixi_header.index('SEX_ID')].value,
                        'n/a'),
                    ixi_row[ixi_header.index('HEIGHT')].value or 'n/a',
                    ixi_row[ixi_header.index('WEIGHT')].value or 'n/a',
                    parse_date(
                        ixi_row[ixi_header.index('DOB')].value
                    ) or 'n/a',
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
                    parse_date(
                        ixi_row[ixi_header.index('STUDY_DATE')].value
                    ) or 'n/a',
                ]
                yield participant

        write_tsv(iter_rows(), path_tsv)
