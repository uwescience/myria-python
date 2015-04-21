from .connection import *
from .errors import *
from .relation import *
from .query import *
from .plans import *
from .schema import *
import cmd

try:
    # IPython is not required, so swallow exception if not installed
    from .extension import *
except ImportError:
    pass

version = "1.2-dev"
