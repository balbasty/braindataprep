"""
Expected input
--------------
IXI/
  sourcedata/
      IXI-T1.tar
      IXI-T2.tar
      IXI-PD.tar
      IXI-MRA.tar
      IXI-DTI.tar
      bvecs.txt
      bvals.txt
      IXI.xls

Expected output
---------------
IXI/
  dataset_description.json
  participants.tsv
  participants.json
  rawdata/
      dwi.bval
      dwi.bvec
      sub-{03d}/
          anat/
              sub-{03d}_T1w.nii.gz
              sub-{03d}_T1w.json
              sub-{03d}_T2w.nii.gz
              sub-{03d}_T2w.json
              sub-{03d}_PDw.nii.gz
              sub-{03d}_PDw.json
              sub-{03d}_angio.nii.gz
              sub-{03d}_angio.json
          dwi/
              sub-{03d}_dwi.nii.gz
              sub-{03d}_dwi.json
"""

from pathlib import Path
from typing import Literal, Iterable

from braindataprep.utils.path import get_tree_path
from braindataprep.utils.log import setup_filelog
from braindataprep.actions import IfExists
from braindataprep.datasets.IXI.command import ixi
from braindataprep.datasets.IXI.bidsifier import Bidsifier

KeyChoice = Literal["meta", "T1w", "T2w", "PDw", "angio", "dwi"]


@ixi.command
def bidsify(
    path: str | Path | None = None,
    *,
    keys: Iterable[KeyChoice] = KeyChoice.__args__,
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
        Path to root of all datasets. An `IXI/sourcedata` folder must exist.
    keys : [list of] {"meta", "T1w", "T2w", "PDw", "angio", "dwi"}
        Only bidsify these keys
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
    root = get_tree_path(path) / 'IXI'

    # Bidsify
    Bidsifier(
        root,
        keys=keys,
        subs=subs,
        exclude_subs=exclude_subs,
        json=json,
        ifexists=if_exists,
    ).run()
