
import sapint

from sapint import SAPException
from sapnwrfc import RFCException
from sapnwrfc import RFCCommunicationError
from sapnwrfc import RFCServerError
from sapnwrfc import RFCFunctionCallError

from sapint import SharedFunction
logger = sapint.getCommentLogger(__name__)

import json

destName = 'AIP'

conn = sapint.GetDestination(destName)

def FunctionInterFace(FUNCNAME,NONE_UNICODE_LENGTH='',LANGUAGE= "1" ):
	fd = conn.discover("RFC_GET_FUNCTION_INTERFACE")
	f = fd.create_function_call()
	f.FUNCNAME(FUNCNAME)
	f.LANGUAGE(LANGUAGE)
	f.NONE_UNICODE_LENGTH(NONE_UNICODE_LENGTH)
	f.invoke()
	PARAMS = f.PARAMS.value
	return sapint.SharedFunction.StripRfcTable(PARAMS)

def FunctionSearch(FUNCNAME,GROUPNAME='',LANGUAGE= "1" ):
	fd = conn.discover("RFC_FUNCTION_SEARCH")
	f = fd.create_function_call()
	f.FUNCNAME(FUNCNAME)
	f.GROUPNAME(GROUPNAME)
	f.LANGUAGE(LANGUAGE)
	f.invoke()
	FUNCTIONS = f.FUNCTIONS.value
	return sapint.SharedFunction.StripRfcTable(FUNCTIONS)

def GroupSearch(GROUPNAME,LANGUAGE="1"):
	fd = conn.discover("RFC_GROUP_SEARCH")
	f = fd.create_function_call()
	f.GROUPNAME(GROUPNAME)
	f.LANGUAGE(LANGUAGE)
	f.invoke()
	GROUPS = f.GROUPS.value
	#print(GROUPS)
	return sapint.SharedFunction.StripRfcTable(GROUPS)

if __name__=="__main__":
	print("test")
	#destName = "VIQAS"
	#grps = GroupSearch("Z*")
	#funs = FunctionSearch("Z*","ZMDM01")
	#print(funs)

	paras = FunctionInterFace("RFC_GROUP_SEARCH")
	logger.info(paras)
	for k in paras:
		print(k)
	conn.close()
	pass