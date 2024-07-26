import tarfile
from logging import getLogger
from pathlib import Path, PosixPath
from typing import Iterable, Iterator, Literal

from braindataprep.pyout import bidsify_tab
from braindataprep.pyout import Status
from braindataprep.utils.io import read_json
from braindataprep.utils.io import write_from_buffer
from braindataprep.freesurfer import bidsify as fs
from braindataprep.actions import IfExists
from braindataprep.actions import Action
from braindataprep.actions import WriteJSON
from braindataprep.actions import WriteBytes
from braindataprep.actions import CopyJSON
from braindataprep.actions import CopyBytes
from braindataprep.datasets.OASIS.III.keys import allleaves
from braindataprep.datasets.OASIS.III.keys import compat_keys
from braindataprep.datasets.OASIS.III.keys import lower_keys

lg = getLogger(__name__)


class Bidsifier:
    """OASIS-III - bidsifying logic"""

    # ------------------------------------------------------------------
    #   Constants
    # ------------------------------------------------------------------

    # Folder containing template README/JSON/...
    TPLDIR = Path(__file__).parent / 'templates'

    # ------------------------------------------------------------------
    #   Initialise
    # ------------------------------------------------------------------
    def __init__(
        self,
        root: Path,
        *,
        keys: Iterable[str] = allleaves,
        exclude_keys: Iterable[str] = set(),
        subs: Iterable[int] = tuple(),
        exclude_subs: Iterable[int] = tuple(),
        json: Literal["yes", "no", "only"] | bool = True,
        ifexists: IfExists.Choice = "skip",
    ):
        self.root: Path = Path(root)
        self.keys: set[str] = set(keys)
        self.exclude_keys: set[str] = set(*map(lower_keys, exclude_keys))
        self.subs: set[int] = set(subs)
        self.exclude_subs: set[int] = set(exclude_subs)
        self.json: Literal["yes", "no", "only"] = (
            "yes" if json is True else
            "no" if json is False else json
        )
        self.ifexists: IfExists.Choice = ifexists

    def init(self):
        """Prepare common stuff"""
        # Printer
        self.out = bidsify_tab()
        # Folder
        self.src = self.root / 'sourcedata'
        self.raw = self.root / 'rawdata'
        self.pheno = self.root / 'phenotypes'
        self.drv = self.root / 'derivatives'
        self.dfs = self.drv / 'oasis-freesurfer'
        # Track errors
        self.nb_errors = 0
        self.nb_skipped = 0

    # ------------------------------------------------------------------
    #   Run all actions
    # ------------------------------------------------------------------
    def run(self):
        """Run all actions"""
        self.init()
        with self.out as self.out:
            self._run()

    def _run(self):
        """Must be run from inside the `out` context."""
        if not self.subs:
            self.subs = set()
            for fname in self.src.glob('OAS3*'):
                id = int(fname.name.split('_')[0][4:])
                self.subs.add(id)
        self.subs -= self.exclude_subs

        # Metadata
        self.nb_errors = self.nb_skipped = 0
        for status in self.make_meta():
            status.setdefault('modality', 'meta')
            self.out(status)

        # Raw and lightly processed data are stored in the same archive
        rawkeys = (allleaves - lower_keys('derivatives')) - lower_keys('meta')
        for key in rawkeys:
            if not (compat_keys(key) & self.keys):
                continue
            if ({key} & self.exclude_keys):
                continue
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_raw(key):
                status.setdefault('modality', key)
                self.out(status)

        # Freesurfer outputs are stored in their own archive
        do_fs = bool(compat_keys('fs') & self.keys)
        do_fs |= bool(compat_keys('fs-all') & self.keys)
        do_fs &= not bool({'fs', 'fs-all'} & self.exclude_keys)
        if do_fs:
            self.nb_errors = self.nb_skipped = 0
            for status in self.make_freesurfer():
                status.setdefault('modality', 'fs')
                self.out(status)

        # TODO: PUP

    # ------------------------------------------------------------------
    #   Helpers
    # ------------------------------------------------------------------
    def fixstatus(self, status: Status, fname: str | Path) -> Iterator[Status]:
        status.setdefault('path', fname)
        yield status
        if status.get('status', '') == 'error':
            self.nb_errors += 1
            yield {'errors': self.nb_errors}
        elif status.get('status', '') == 'skipped':
            self.nb_skipped += 1
            yield {'skipped': self.nb_skipped}

    # ------------------------------------------------------------------
    #   Write metadata files
    # ------------------------------------------------------------------
    def make_meta(self) -> Iterator[Status]:
        # Register future actions
        actions = {}

        if compat_keys('meta') & self.keys:
            actions = {
                **actions,
                'README':
                    CopyBytes(
                        self.TPLDIR / 'README',
                        self.root / 'README',
                    ),
                'dataset_description.json':
                    CopyJSON(
                        self.TPLDIR / 'dataset_description.json',
                        self.root / 'dataset_description.json',
                    ),
                'participants.json':
                    CopyJSON(
                        self.TPLDIR / 'participants.json',
                        self.root / 'participants.json',
                    ),
                'sessions.json':
                    CopyJSON(
                        self.TPLDIR / 'sessions.json',
                        self.root / 'sessions.json',
                    ),
            }

        # Register Phenotypes actions
        if compat_keys('peno') & self.keys:
            for phenotype in ('UDSv2', 'UDSv3'):
                json_pheno = self.TPLDIR / 'phenotypes' / phenotype
                for fname in json_pheno.glob('*.json'):
                    ofname = self.pheno / fname.name
                    if 'a3' in fname.name:
                        # Duplicate parents/siblings
                        obj = read_json(fname)
                        for key, val in dict(obj).items():
                            if "{d}" not in key:
                                continue
                            assert key.startswith(("SIB", "KID", "REL"))
                            nb = 20 if key.startswith("SIB") else 15
                            del obj[key]
                            desc = val.pop("Description")
                            for d in range(nb):
                                obj[key.format(d=d)] = {
                                    'Description': desc.format(d=d),
                                    **val
                                }
                        actions[ofname.fname] = WriteJSON(obj, ofname,
                                                          src=fname)
                    else:
                        actions[ofname.name] = CopyJSON(fname, ofname)

        # Register Freesurfer actions
        fskeys = compat_keys('fs') | compat_keys('fs-all')
        if fskeys & self.keys:
            for action in fs.bidsify_toplevel(self.dfs, (5, 3)):
                actions[str(action.dst.name)] = action

        # Perform actions
        yield {'progress': 0}
        with IfExists(self.ifexists):
            for i, (fname, action) in enumerate(actions.items()):
                for status in action:
                    yield from self.fixstatus(status, fname)
                yield {'progress': 100*(i+1)/len(actions)}
        yield {'progress': 100}

        yield {'status': 'done', 'message': ''}

    # ------------------------------------------------------------------
    #   Write rawdata
    # ------------------------------------------------------------------
    def make_raw(self, key):

        # cat:      OASIS category    -- in folder: OAS3{id}_{cat}_{ses}/
        # subcat:   OASIS subcategory -- in filename: {subcat}{n}.nii.gz
        # bidscat:  BIDS category     -- in folder: {bidscat}/
        # bidsmod:  BIDS modality     -- in filename: sub-{id}_{bidsmod}.nii.gz
        # bidsacq:  BIDS acquisition  -- in filename: acq-{bidsacq}_pet.nii.gz
        cat = subcat = bidscat = bidsmod = bidsacq = None
        if key in lower_keys('mri'):
            cat = 'MR'
            bidsmod = key
            if key in lower_keys('anat'):
                bidscat = 'anat'
                if key in lower_keys('swi'):
                    subcat = 'swi'
                else:
                    subcat = 'anat'
            elif key in lower_keys('func'):
                bidscat = 'func'
                subcat = 'func'
            elif key in lower_keys('perf'):
                bidscat = 'perf'
                subcat = 'func'
            else:
                subcat = key
                if key in ('fmap', 'fieldmap'):
                    bidscat = 'fmap'
                elif key == 'dwi':
                    bidscat = 'dwi'
                else:
                    assert False
        elif key in lower_keys('pet'):
            subcat = bidscat = bidsmod = 'pet'
            if key in lower_keys('fdg'):
                cat = bidsacq = 'FDG'
            elif key in lower_keys('pib'):
                cat = bidsacq = 'PIB'
            elif key in lower_keys('av45'):
                cat = bidsacq = 'AV45'
            elif key in lower_keys('av1451'):
                cat = bidsacq = 'AV1451'
            else:
                assert False
        elif key in lower_keys('ct'):
            cat = subcat = bidsmod = 'CT'
            bidscat = 'ct'
        else:
            assert False, f"{key} not an MR/PET/CT"

        # Run actions
        yield {'progress': 0}
        for i, id in enumerate(self.subs):
            for action in self._make_raw(
                cat, subcat, bidscat, bidsmod, bidsacq, id
            ):
                for status in action:
                    yield from self.fixstatus(status, action.dst.name)
            yield {'progress': 100*(i+1)/len(self.subs)}
        yield {'status': 'done', 'message': ''}

    def _make_raw(self, cat, subcat, bidscat, bidsmod, bidsacq, id):
        """Process one subject"""
        paths = self.src.glob(f'OAS3{id:04d}_{cat}_*/{subcat}*.tar.gz')
        for path in paths:
            try:
                with tarfile.open(path, 'r:gz') as tar:
                    yield from self._make_raw_scan(
                        tar, bidscat, bidsmod, bidsacq, id
                    )
            except Exception as e:
                lg.error(f"{path}: {e}")

    def _make_raw_scan(self, tar, bidscat, bidsmod, bidsacq, id):
        members = tar.getnames()
        if not any(x.endswith(f'_{bidsmod}.nii.gz') for x in members):
            return
        if bidsacq and not any(f'_acq-{bidsacq}_' in x for x in members):
            return
        for member in tar.getmembers():
            membername = PosixPath(member.name)
            flags = membername.name.split('_')
            for flag in flags:
                flag = flag.split('-')
                if flag[0] in ('ses', 'sess'):
                    ses = flag[1]
                    break
            dst = self.raw / f'sub-{id:04d}' / f'ses-{ses}' / bidscat
            mname = self.fix_name(membername.name, id)
            if (
                (mname.endswith('.json') and self.json != 'no')
                or
                (mname.endswith('.nii.gz') and self.json != 'only')
            ):
                yield Action(
                    tar.name, dst / mname,
                    lambda f:
                        write_from_buffer(tar.extractfile(member), f)
                )

    def fix_name(self, name, id):
        substitutions = {
            'sess-': 'ses-',
            'sub-OAS3{id:04d}': f'sub-{id:04d}',
            'task-restingstateMB4': 'task-restingstate_acq-MB4',
        }
        for old, new in substitutions.items():
            name = name.replace(old, new)
        return name

    # ------------------------------------------------------------------
    #   Write freesurfer
    # ------------------------------------------------------------------
    def make_freesurfer(self):
        # Run actions
        yield {'progress': 0}
        for i, id in enumerate(self.subs):
            for action in self._make_freesurfer(id):
                for status in action:
                    yield from self.fixstatus(status, action.dst.name)
                yield {'progress': 100*(i+1)/len(self.subs)}
        yield {'progress': 100}
        yield {'status': 'done', 'message': ''}

    def _make_freesurfer(self, id):
        """Process one subject"""
        paths = self.src.glob(f'OAS3{id:04d}_MR_*/*Freesurfer*.tar.gz')
        for path in paths:
            ses = path.name.split('.')[0].split('_')[-1]

            # Unpack raw freesurfer outputs
            # under "derivatives/oasis-freesurfer/sourcedata/sub-{04d}/ses-{}"
            with tarfile.open(str(path), 'r:gz') as tar:
                for member in tar.getmembers():
                    tarpath = PosixPath(member.name)
                    if 'fs-all' not in self.keys:
                        if not str(tarpath).endswith(fs.bidsifiable_outputs):
                            continue
                    dst = self.dfs/'sourcedata'/f'sub-{id:04d}'/f'ses-{ses}'
                    dst = dst.joinpath(*tarpath.parts[6:])
                    yield WriteBytes(
                        tar.extractfile(member),
                        dst,
                        src=tar.name,
                    )

            # Bidsify under "derivatives/oasis-freesurfer/sub-{04d}/ses-{}"
            src = self.dfs / 'sourcedata' / f'sub-{id:04d}' / f'ses-{ses}'
            dst = self.dfs / f'sub-{id:04d}' / f'ses-{ses}'
            srcbase = f'bids:raw:sub-{id:04d}/anat/sub-{id:04d}/ses-{ses}/'
            sourcefiles = [srcbase + 'sub-{id:04d}_ses-{ses}_T1w.nii.gz']
            yield from fs.bidsify(src, dst, sourcefiles, json=self.json)
