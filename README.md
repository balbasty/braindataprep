# BrainDataPrep

A single application to download, bidsify and preprocess public datasets

This is a very early work in progress.

### Funding
This application is being developed as part of the project
_"High-resolution spatiotemporal models of the brain across the lifespan
for diagnosis and decision-making"_, funded by a fellowship of the
Royal Society (NIF\R1\232460).

# Installation

```shell
pip install "braindataprep @ git+https://github.com/balbasty/braindataprep"
```

Specific datasets may require additional dependencies. To install the
dependencies to a specific dataset, use the corresponding tag. For example:
```shell
pip install "braindataprep[ixi] @ git+https://github.com/balbasty/braindataprep"
```
To install all possible dependencies, use the `all` tag:
```shell
pip install "braindataprep[all] @ git+https://github.com/balbasty/braindataprep"
```

# Usage

After installation, the `bdp` command is available from your shell.
Each known dataset is a specific command, with a set of subcommands.
These datasets can be listed by typing `bdp`:
```
Usage: bdp COMMAND

┏━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┓
┃           BrainDataPrep : Download, Bidsify and Process public datasets           ┃
┗━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━┛

╭─ Commands ────────────────────────────────────────────────────────────────────────╮
│ ixi        Commands related to the IXI dataset                                    │
│ oasis1     Commands related to the OASIS-I dataset                                │
│ oasis2     Commands related to the OASIS-II dataset                               │
│ --help,-h  Display this message and exit.                                         │
│ --version  Display application version.                                           │
╰───────────────────────────────────────────────────────────────────────────────────╯
```

## IXI: Information eXtraction from Images

```
Usage: bdp ixi COMMAND

Commands related to the IXI dataset

 • Project       Information eXtraction from Images (IXI)
 • Modalities    T1w, T2w, PD2, MRA, DTI
 • Populations   Controls
 • Funding       EPSRC GR/S21533/02
 • License       CC BY-SA 3.0

╭─ Commands ────────────────────────────────────────────────────────────────────────╮
│ download   Download source data for the IXI dataset.                              │
│ bidsify    Convert source data into a BIDS-compliant directory                    │
│ --help,-h  Display this message and exit.                                         │
│ --version  Display application version.                                           │
╰───────────────────────────────────────────────────────────────────────────────────╯

Usage: bdp ixi download [ARGS] [OPTIONS]

Download source data for the IXI dataset.
╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path  Path to root of all datasets. An IXI folder will be created.         │
│ --keys       Modalities to download                                               │
│              [default: ('meta', 'T1w', 'T2w','PDw', 'angio', 'dwi')]              │
│ --if-exists  Behaviour if a file already exists                                   │
│              [choices: skip,overwrite,different,refresh,error] [default: skip]    │
│ --packet     Packet size to download, in bytes [default: 8.4 MB]                  │
│ --log        Path to log file                                                     │
╰───────────────────────────────────────────────────────────────────────────────────╯

Usage: bdp ixi bidsify [ARGS] [OPTIONS]

Convert source data into a BIDS-compliant directory

╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path     Path to root of all datasets. An IXI/sourcedata folder            │
│                 must exist.                                                       │
│ --keys          Only bidsify these keys                                           │
│                 [default: ('meta', 'T1w', 'T2w','PDw', 'angio', 'dwi')]           │
│ --subs          Only bidsify these subjects (all if empty) [default: ()]          │
│ --exclude-subs  Do not bidsify these subjects [default: ()]                       │
│ --json          Whether to write (only) sidecar JSON files                        │
│                 [choices:yes,no,only] [default: yes]                              │
│ --if-exists     Behaviour when a file already exists                              │
│                 [choices: skip,overwrite,different,refresh,error] [default: skip] │
│ --log           Path to log file                                                  │
╰───────────────────────────────────────────────────────────────────────────────────╯
```

## OASIS: Open Access Series of Imaging Studies

## OASIS-1: Cross-sectional data across the adult lifespan

```
Usage: bdp oasis1 COMMAND

Commands related to the OASIS-I dataset

 • Project       Open Access Series of Imaging Studies (OASIS)
 • Subproject    Cross-sectional data across the adult lifespan (OASIS-I)
 • Modalities    T1w
 • Populations   Controls, Dementia
 • Funding       NIH: P50 AG05681, P01 AG03991, P01 AG026276, R01 AG021910,
   P20 MH071616, U24 RR021382
 • Reference     https://doi.org/10.1162/jocn.2007.19.9.1498

╭─ Commands ────────────────────────────────────────────────────────────────────────╮
│ download   Download source data for the OASIS-I dataset.                          │
│ bidsify    Convert source data into a BIDS-compliant directory                    │
│ --help,-h  Display this message and exit.                                         │
│ --version  Display application version.                                           │
╰───────────────────────────────────────────────────────────────────────────────────╯

Usage: bdp oasis1 download [ARGS] [OPTIONS]

Download source data for the OASIS-I dataset.

Possible keys:
 • raw          All the raw imaging data
 • fs           Data processed with FreeSurfer
 • meta         Metadata
 • reliability  Repeatability measures data sheet
 • facts        Fact sheet

╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path  Path to root of all datasets. An OASIS-1 folder will be created.     │
│ --keys       Data categories to download                                          │
│              [default: ('raw', 'fs', 'meta', 'reliability', 'facts')]             │
│ --discs      Discs to download [default: (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)] │
│ --if-exists  Behaviour if a file already exists                                   │
│              [choices: skip,overwrite,different,refresh,error] [default: skip]    │
│ --packet     Packet size to download, in bytes [default: 8.4 MB]                  │
│ --log        Path to log file                                                     │
╰───────────────────────────────────────────────────────────────────────────────────╯

Usage: bdp oasis1 bidsify [ARGS] [OPTIONS]

Convert source data into a BIDS-compliant directory

╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path     Path to root of all datasets. An OASIS-1/sourcedata folder        │
│                 must exist.                                                       │
│ --keys          Only bidsify these keys                                           │
│                 [default: ('meta', 'raw', 'avg', 'tal', 'fsl', 'fs', 'fs-all')]   │
│ --discs         Discs to bidsify.                                                 │
│                 [default: (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12)]                │
│ --subs          Only bidsify these subjects (all if empty) [default: ()]          │
│ --exclude-subs  Do not bidsify these subjects [default: ()]                       │
│ --json          Whether to write (only) sidecar JSON files                        │
│                 [choices: yes,no,only] [default: yes]                             │
│ --if-exists     Behaviour when a file already exists                              │
│                 [choices: skip,overwrite,different,refresh,error] [default: skip] │
│ --log           Path to log file                                                  │
╰───────────────────────────────────────────────────────────────────────────────────╯
```

### OASIS-2: Longitudinal MRI Data in Nondemented and Demented Older Adults

```
Usage: bdp oasis2 COMMAND

Commands related to the OASIS-II dataset

 • Project       Open Access Series of Imaging Studies (OASIS)
 • Subproject    Longitudinal MRI Data in Nondemented and Demented Older Adults
                 (OASIS-II)
 • Modalities    T1w
 • Populations   Controls, Dementia
 • Funding       NIH: P50 AG05681,  P01 AG03991,  P01 AG026276,
                      R01 AG021910, P20 MH071616, U24 RR021382
 • Reference     https://doi.org/10.1162/jocn.2009.21407

╭─ Commands ────────────────────────────────────────────────────────────────────────╮
│ download   Download source data for the OASIS-II dataset.                         │
│ bidsify    Convert source data into a BIDS-compliant directory                    │
│ --help,-h  Display this message and exit.                                         │
│ --version  Display application version.                                           │
╰───────────────────────────────────────────────────────────────────────────────────╯

Usage: bdp oasis2 download [ARGS] [OPTIONS]

Download source data for the OASIS-II dataset.

Possible keys:
 • raw          All the raw imaging data
 • fs           Data processed with FreeSurfer
 • meta         Metadata
 • reliability  Repeatability measures data sheet
 • facts        Fact sheet

╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path  Path to root of all datasets. An OASIS-2 folder will be created.     │
│ --keys       Data categories to download [default: ('raw', 'meta')]               │
│ --parts      Parts to download [default: (1, 2)]                                  │
│ --if-exists  Behaviour if a file already exists                                   │
│              [choices: skip,overwrite,different,refresh,error] [default: skip]    │
│ --packet     Packet size to download, in bytes [default: 8.4 MB]                  │
│ --log        Path to log file                                                     │
╰───────────────────────────────────────────────────────────────────────────────────╯

Usage: bdp oasis2 bidsify [ARGS] [OPTIONS]

Convert source data into a BIDS-compliant directory

╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path     Path to root of all datasets. An OASIS-2/sourcedata folder must   │
│                 exist.                                                            │
│ --keys          Only bidsify these keys [default: ('meta', 'raw')]                │
│ --parts         Parts to bidsify. [default: (1, 2)]                               │
│ --subs          Only bidsify these subjects (all if empty) [default: ()]          │
│ --exclude-subs  Do not bidsify these subjects [default: ()]                       │
│ --json          Whether to write (only) sidecar JSON files                        │
│                 [choices: yes,no,only] [default: yes]                             │
│ --if-exists     Behaviour when a file already exists                              │
│                 [choices: skip,overwrite,different,refresh,error] [default: skip] │
│ --log           Path to log file                                                  │
╰───────────────────────────────────────────────────────────────────────────────────╯
```

### OASIS-3: Longitudinal Multimodal Neuroimaging, Clinical, and Cognitive Dataset for Normal Aging and Alzheimer's Disease

```
Usage: bdp oasis3 COMMAND

Commands related to the OASIS-III dataset

 • Project       Open Access Series of Imaging Studies (OASIS)
 • Subproject    Longitudinal Multimodal Neuroimaging, Clinical, and Cognitive
                 Dataset for Normal Aging and Alzheimer's Disease (OASIS-III)
 • Modalities    T1w, T2w, TSE, FLAIR, T2star, angio, pasl, asl, bold,
                dwi, swi, fdg, pib, av45, av1451, CT
 • Populations   Controls, Dementia
 • Funding       NIH: P30 AG066444, P50 AG00561,  P30 NS09857781, P01 AG026276,
                      P01 AG003991, R01 AG043434, UL1 TR000448,   R01 EB009352
 • Reference     https://doi.org/10.1101/2019.12.13.19014902

╭─ Commands ────────────────────────────────────────────────────────────────────────╮
│ download   Download source data for the OASIS-III dataset.                        │
│ bidsify    Convert source data into a BIDS-compliant directory                    │
│ --help,-h  Display this message and exit.                                         │
│ --version  Display application version.                                           │
╰───────────────────────────────────────────────────────────────────────────────────╯

Usage: bdp oasis3 download [ARGS] [OPTIONS]

Download source data for the OASIS-III dataset.

Hierarchy of keys:
 • raw :              All the raw imaging data
    • mri :           All the MRI data
       • anat :       All the anatomical MRI data
          • T1w :     T1-weighted MRI scans
          • T2w :     T2-weighted MRI scans
          • TSE :     Turbo Spin Echo MRI scans
          • FLAIR :   Fluid-inversion Recovery MRI scans
          • T2star :  T2-star quantitative scans
          • angio :   MR angiography scans
          • swi :     All susceptibility-weighted MRI data
       • func :       All fthe functional MRI data
          • pasl :    Pulsed arterial spin labeling
          • asl :     Arterial spin labelling
          • bold :    Blood-oxygenation level dependant (fMRI) scans
       • fmap :       All field maps
       • dwi :        All diffusion-weighted MRI data
    • pet :           All the PET data
       • fdg :        Fludeoxyglucose
       • pib :        Pittsburgh Compound B (amyloid)
       • av45 :       18F Florpiramine (tau)
       • av1451 :     18F Flortaucipir (tau)
    • ct              All the CT data
 • derivatives :      All derivatives
    • fs :            Freesurfer derivatives
    • pup :           PET derivatives
 • meta :             All metadata
    • pheno :         Phenotypes

╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path     Path to root of all datasets. An OASIS-2 folder will be created.  │
│ --keys          Data categories to download [default: ()]                         │
│ --subs          Only bidsify these subjects (all if empty) [default: ()]          │
│ --exclude-subs  Do not bidsify these subjects [default: ()]                       │
│ --if-exists     Behaviour if a file already exists                                │
│                 [choices: skip,overwrite,different,refresh,error] [default: skip] │
│ --user          NITRC username                                                    │
│ --password      NITRC password                                                    │
│ --packet        Packet size to download, in bytes [default: 8.4 MB]               │
│ --log           Path to log file                                                  │
╰───────────────────────────────────────────────────────────────────────────────────╯

Usage: bdp oasis3 bidsify [ARGS] [OPTIONS]

Convert source data into a BIDS-compliant directory

Hierarchy of keys:

 • raw :              All the raw imaging data
    • mri :           All the MRI data
       • anat :       All the anatomical MRI data
          • T1w :     T1-weighted MRI scans
          • T2w :     T2-weighted MRI scans
          • TSE :     Turbo Spin Echo MRI scans
          • FLAIR :   Fluid-inversion Recovery MRI scans
          • T2star :  T2-star quantitative scans
          • angio :   MR angiography scans
          • swi :     All susceptibility-weighted MRI data
       • func :       All fthe functional MRI data
          • pasl :    Pulsed arterial spin labeling
          • asl :     Arterial spin labelling
          • bold :    Blood-oxygenation level dependant (fMRI) scans
       • fmap :       All field maps
       • dwi :        All diffusion-weighted MRI data
    • pet :           All the PET data
       • fdg :        Fludeoxyglucose
       • pib :        Pittsburgh Compound B (amyloid)
       • av45 :       18F Florpiramine (tau)
       • av1451 :     18F Flortaucipir (tau)
    • ct              All the CT data
 • derivatives :      All derivatives
    • fs :            Freesurfer derivatives
    • fs-all :        Freesurfer derivatives (even non bidsifiable ones)
    • pup :           PET derivatives
 • meta :             All metadata
    • pheno :         Phenotypes

╭─ Parameters ──────────────────────────────────────────────────────────────────────╮
│ PATH,--path     Path to root of all datasets. An OASIS-2/sourcedata folder must   │
│                 exist.                                                            │
│ --keys          Only bidsify these keys (all if empty) [default: ()]              │
│ --exclude-keys  Do not bidsify these keys [default: ()]                           │
│ --subs          Only bidsify these subjects (all if empty) [default: ()]          │
│ --exclude-subs  Do not bidsify these subjects [default: ()]                       │
│ --json          Whether to write (only) sidecar JSON files                        │
│                 [choices: yes,no,only] [default: yes]                             │
│ --if-exists     Behaviour when a file already exists                              │
│                 [choices: skip,overwrite,different,refresh,error] [default: skip] │
│ --log           Path to log file                                                  │
╰───────────────────────────────────────────────────────────────────────────────────╯
```
