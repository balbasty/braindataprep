# brain-data-preproc
Scripts to download, bidsify and preprocess public datasets


# Installation

```shell
pip install "braindataprep @ git+https://github.com/balbasty/braindataprep"
```

Specific datasets may require additional dependencies. To install the
dependencies to a specific dataset, use the correpsonding taf. For example:
```shell
pip install "braindataprep[ixi] @ git+https://github.com/balbasty/braindataprep"
```
To install all possible dependencies, use the `all` tag:
```shell
pip install "braindataprep[all] @ git+https://github.com/balbasty/braindataprep"
```

# Usage

```
--------------------------------------------------------------------------------
Usage: python -m braindataprep.datasets.IXI.download [OPTIONS]

Options:
  --path TEXT                     Path to tree
  --key [meta|T1w|T2w|PDw|MRA|DTI]
                                  Modalities to download
  --packet INTEGER                Packet size to download
  --help                          Show this message and exit
--------------------------------------------------------------------------------
Usage: python -m braindataprep.datasets.IXI.bidsify [OPTIONS]

Options:
  --path TEXT                     Path to tree
  --key [meta|json|T1w|T2w|PDw|angio|swi]
                                  Only bidsify these keys
  --json-only                     Only write jsons (not volumes)
  --help                          Show this message and exit.
--------------------------------------------------------------------------------
Usage: python -m braindataprep.datasets.OASIS.I.download [OPTIONS]

Options:
  --path TEXT                     Path to tree
  --packet INTEGER                Packet size for download
  --key [raw|fs|meta|reliability|facts]
                                  Only download these keys
  --disc INTEGER                  Only download these discs (1..12)
  --help                          Show this message and exit.
--------------------------------------------------------------------------------
Usage: python -m braindataprep.datasets.OASIS.I.bidsify [OPTIONS]

Options:
  --path TEXT                     Path to tree
  --key [meta|raw|average|talairach|fsl|fs]
                                  Only download these keys
  --disc INTEGER                  Only download these discs (1..12)
  --sub INTEGER                   Only download these subjects
  --all-fs                        Unpack all FS sourcedata (even files that
                                  cannot be bidsified)
  --json-only                     Only write jsons (not volumes)
  --help                          Show this message and exit
--------------------------------------------------------------------------------
```
