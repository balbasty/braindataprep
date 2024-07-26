from cyclopts import App
from braindataprep.cli import app

oasis2_help = """
Commands related to the OASIS-II dataset

* **Project**       Open Access Series of Imaging Studies (OASIS)
* **Subproject**    Longitudinal MRI Data in Nondemented and Demented Older Adults (OASIS-II)
* **Modalities**    T1w
* **Populations**   Controls, Dementia
* **Funding**       NIH: P50 AG05681,  P01 AG03991,  P01 AG026276,
                         R01 AG021910, P20 MH071616, U24 RR021382
* **Reference**     https://doi.org/10.1162/jocn.2009.21407
"""  # noqa: E501

app.command(oasis2 := App(name="oasis2", help=oasis2_help))
