from cyclopts import App
from braindataprep.cli import app

oasis1_help = """
Commands related to the OASIS-I dataset

* **Project**       Open Access Series of Imaging Studies (OASIS)
* **Subproject**    Cross-sectional data across the adult lifespan (OASIS-I)
* **Modalities**    T1w
* **Populations**   Controls, Dementia
* **Funding**       NIH: P50 AG05681, P01 AG03991, P01 AG026276,
                    R01 AG021910, P20 MH071616, U24 RR021382
* **Reference**     https://doi.org/10.1162/jocn.2007.19.9.1498
"""

app.command(oasis1 := App(name="oasis1", help=oasis1_help))
