import logging

logging.basicConfig(format='%(levelname)s | %(message)s', level=15)


# discover usable datasets
try: from .datasets import IXI
except: pass
try: from .datasets.OASIS import I
except: pass
try: from .datasets.OASIS import II
except: pass
try: from .datasets.OASIS import III
except: pass
