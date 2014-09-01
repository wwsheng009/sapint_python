
import sapint

from sapint import SAPException
from sapnwrfc import RFCException
from sapnwrfc import RFCCommunicationError
from sapnwrfc import RFCServerError
from sapnwrfc import RFCFunctionCallError

from sapint import SharedFunction
import json
logger = sapint.getCommentLogger(__name__)
#----------------------------------------------------------------------
def tablefieldsinfo(destName,TableName,FieldName=None,
                    Language=None):
	""""""
	try:
		sapdes = sapint.GetDestination(destName)
		fd = sapdes.discover("DDIF_FIELDINFO_GET")
		f = fd.create_function_call()
		f.TABNAME(TableName)
		if FieldName!=None:
			f.FIELDNAME(FieldName)
		if Language!=None:
			f.LANGU(Language)

		f.invoke()
		fields = f.DFIES_TAB.value
		#print(des)
		#return fields
		return sapint.SharedFunction.StripRfcTable(fields)

	except RFCCommunicationError as x:
		print(dir(x))
		print(x.args)
	except Exception as ex:
		template = "An exception of type {0} occured. Arguments:\n{1!r}"
		message = template.format(type(ex).__name__, ex.args)
		print(message)


if __name__=="__main__":
	print("test reading the table fields infomation")
	fields = tablefieldsinfo("AIP","MARA","MATNR")
	#print(fields)
	#print("xxx")

	original = json.dumps(fields,ensure_ascii=False)    

	#original = json.dumps(fields, skipkeys=False, ensure_ascii=False, check_circular=True, 
				#allow_nan=True, cls=None, indent=None, 
				#separators=None, default=None, 
				#sort_keys=False)
	logger.info(original)
	#print(original)
	pass