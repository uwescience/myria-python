__import__('pkg_resources').declare_namespace(__name__)

from .connection import *
from .errors import *
from .relation import *
from .query import *
from .schema import *
import cmd

# IPython is not required, so swallow exception if not installed
try:
    from .extension import *
except ImportError:
    pass

version = "1.2-dev"
