
import sapint

from sapint import SAPException
from sapnwrfc import RFCException
from sapnwrfc import RFCCommunicationError
from sapnwrfc import RFCServerError
from sapnwrfc import RFCFunctionCallError

from sapint import SharedFunction
logger = sapint.getCommentLogger(__name__)

import json


#----------------------------------------------------------------------
def testfunction1(destName,FunctionName):
    """"""
    try:
        conn = sapint.GetDestination(destName)
        
        FunctionName = FunctionName.upper()
        fd = conn.discover(FunctionName)
        f = fd.create_function_call()
        parameters = f.handle.function_descriptor.parameters
        #print(parameters)
        
        for k,v in parameters.items():
            print(parameters[k])
            #print(f(k))
        #print(f.handle.parameters)
        #for k, v in fd.handle.parameters.items():
            #print(k,v)
            
        
        ##f.TABNAME(TableName)
        #f.invoke()
        #des = f.DFIES_TAB.value
        ##print(des)
        return f.handle.parameters
    except RFCCommunicationError as x:
        print(dir(x))
        print(x.args)
    except Exception as ex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
    

if __name__=="__main__":
    print("test")
    des = testfunction1("EYANGDEV","DDIF_FIELDINFO_GET")
    #print(des)
    #logger.info(json.dumps(des))
    pass