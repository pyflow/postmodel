
__version__ = '0.1.0'

import sys

if sys.version_info < (3, 6):  # pragma: nocoverage
    raise RuntimeError("Postmodel requires Python 3.6")