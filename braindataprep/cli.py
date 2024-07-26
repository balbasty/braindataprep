import cyclopts

_help = """
BrainDataPrep : Download, Bidsify and Process public datasets
=============================================================
"""

runapp = app = cyclopts.App("bdp", help=_help, help_format="markdown")

# def runapp(*a, **k):
#     return app(exit_on_error=False)

if __name__ == "__main__":
    runapp()
