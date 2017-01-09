"""Creating User Defined functions"""
import json
from .errors import MyriaError
from myria  import cloudpickle
import base64

class functionTypes(object):
  POSTGRES = 0
  PYTHON = 1

def create_function(name, description, outType, lang, isMultivalued=False, binary=None):
    body = None

    if(lang == functionTypes.POSTGRES):
        body = {'name': name,
                'description': description,
                'outputSchema': outType,
                'isMultivalued': isMultivalued,
                'lang': functionTypes.POSTGRES}
    elif(lang == functionTypes.PYTHON):
        if(binary is None or outType is None):
            raise MyriaError("Cannot create a python function without binary or output type .")
        else :
            obj = cloudpickle.dumps(binary, 2)
            bo = base64.urlsafe_b64encode(obj)
            body = {'name': name,
                    'description': description,
                    'outputSchema': outType,
                    'isMultivalued':isMultivalued,
                    'lang': functionTypes.PYTHON,
                    'binary': bo }

    if (body==None):
        raise MyriaError("Unsupported language for user function.")
    return body
