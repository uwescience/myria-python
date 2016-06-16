"""User Defined functions"""
import json
from .errors import MyriaError
import cloud
import base64


class functionTypes(object):
  POSTGRES = 0
  PYTHON = 1

def create_function(name,text, outSchema,inSchema,lang,binary=None):

    body = None

    if(lang==functionTypes.POSTGRES):
        body = {'name': name,
                'text': text,
                'outputSchema': outSchema.to_dict(),
                'inputSchema':inSchema.to_dict(),
                'lang': functionTypes.POSTGRES}
    elif(lang==functionTypes.PYTHON):
        if(binary==None or inSchema==None):
            raise MyriaError("Cannot create a python function without binary or input schema.")
        else :
            obj = cloud.serialization.cloudpickle.dumps(binary, 2)
            bo = base64.urlsafe_b64encode(obj)
            body = {'name':name,
                    'text':text,
                    'outputSchema':outSchema.to_dict(),
                    'inputSchema':inSchema.to_dict(),
                    'lang':functionTypes.PYTHON,
                    'binary': bo }

    if (body==None):
        raise MyriaError("Unsupported language for user function.")
    print "Body of message"
    print body
    return body
