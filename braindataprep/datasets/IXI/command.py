from cyclopts import App
from braindataprep.cli import app

ixi_help = """
Commands related to the IXI dataset

* **Project**       Information eXtraction from Images (IXI)
* **Modalities**    T1w, T2w, PD2, MRA, DTI
* **Populations**   Controls
* **Funding**       EPSRC GR/S21533/02
* **License**       CC BY-SA 3.0
"""

app.command(ixi := App(name="ixi", help=ixi_help))
