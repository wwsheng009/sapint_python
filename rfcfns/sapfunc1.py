
import sapint

from sapint import SAPException
from sapnwrfc import RFCException
from sapnwrfc import RFCCommunicationError
from sapnwrfc import RFCServerError
from sapnwrfc import RFCFunctionCallError

from sapint import SharedFunction
#----------------------------------------------------------------------
def testfunction1(destName,TableName):
    """"""
    try:
        sapdes = sapint.GetDestination("EYANGDEV")
        fd = sapdes.discover("DDIF_FIELDINFO_GET")
        f = fd.create_function_call()
        #f.TABNAME(TableName)
        f.invoke()
        des = f.DFIES_TAB.value
        #print(des)
        print(sapint.SharedFunction.StripRfcTable(des))
    except RFCCommunicationError as x:
        print(dir(x))
        print(x.args)
    except Exception as ex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
        print(message)
    

if __name__=="__main__":
    print("test")
    testfunction1("EYANGDEV","MARA")
    pass