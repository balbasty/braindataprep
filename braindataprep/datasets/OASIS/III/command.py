from cyclopts import App
from braindataprep.cli import app

oasis3_help = """
Commands related to the OASIS-III dataset

* **Project**       Open Access Series of Imaging Studies (OASIS)
* **Subproject**    Longitudinal Multimodal Neuroimaging, Clinical,
                    and Cognitive Dataset for Normal Aging and
                    Alzheimer's Disease (OASIS-III)
* **Modalities**    T1w, T2w, TSE, FLAIR, T2star, angio, pasl, asl, bold,
                    dwi, swi, fdg, pib, av45, av1451
* **Populations**   Controls, Dementia
* **Funding**       NIH: P30 AG066444, P50 AG00561,  P30 NS09857781,
                         P01 AG026276, P01 AG003991, R01 AG043434,
                         UL1 TR000448, R01 EB009352
* **Reference**     https://doi.org/10.1101/2019.12.13.19014902
"""  # noqa: E501

app.command(oasis3 := App(name="oasis3", help=oasis3_help))
