"""
Expected input
--------------
OASIS-2/
    sourcedata/
        OAS2_RAW_PART{1|2}.tar.gz
        oasis_longitudinal_demographics.xlsx

Expected output
---------------
OASIS-2/
    dataset_description.json
    participants.{tsv|json}
    sessions.json
    rawdata/
        sub-{04d}/
            sub-{04d}_sessions.tsv
            ses-{d}/
                anat/
                    sub-{04d}_run-{d}_T1w.{nii.gz|json}
"""
from pathlib import Path
from typing import Literal, Iterable

from braindataprep.utils.log import setup_filelog
from braindataprep.utils.path import get_tree_path
from braindataprep.actions.action import IfExists
from braindataprep.datasets.OASIS.II.command import oasis2
from braindataprep.datasets.OASIS.II.bidsifier import Bidsifier

KeyChoice = Literal["meta", "raw"]


@oasis2.command
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.__args__,
    parts: Iterable[int] | None = (1, 2),
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
        Path to root of all datasets. An `OASIS-2/sourcedata` folder must
        exist.
    keys : [list of] {"meta", "raw", "avg", "tal", "fsl", "fs", "fs-all"}
        Only bidsify these keys
    parts : [list of] {1, 2}
        Parts to bidsify.
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
    if isinstance(parts, int):
        parts = [parts]
    parts = list(parts or [])

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
    root = get_tree_path(path) / 'OASIS-2'

    # Bidsify
    Bidsifier(
        root,
        keys=keys,
        parts=parts,
        subs=subs,
        exclude_subs=exclude_subs,
        json=json,
        ifexists=if_exists,
    ).run()
