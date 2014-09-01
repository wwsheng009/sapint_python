# -*- coding: utf-8 -*-

import sys


import datetime
from datetime import *


import sapint
from sapint import SAPException

import json


logger = None
logger = sapint.getCommentLogger(__name__)

from ReadTable import CReadTable

class CBigTable(object):
    Delimiter = ''
    __FetchedRows = 0
    __FieldsOut = []

    __FieldsIn = []
    __Options = []
    
    FunctionName = 'ZVI_BIG_TABLE_SUBMIT'
    RowCount = 0
    RowSkip = 0
    TableName = ''
    Delimiter = None
    __WhereClause = ''
    
    __DestName = ''
    __Dest = object

    Result = []
    

    def __init__(self, pSapSystem):
        logger.info('class CBigTable is initialing....the sap system is {0}'.format(pSapSystem))
        self.__FieldsIn = []
        self.__Options = []
        self.__FieldsOut = []
        
#        在SAP中每个文件的条数。
        self.FileLines = 10000
        self.RowCount = 0 
        self.RowSkip = 0
        self.NoData = ''
        self.__DestName = pSapSystem
        
        if self.__DestName != None and self.__DestName != '':
            self.init()
        else:
            raise SAPException('destination name is need')
        
        self.__tableDef = []
        
        self.Return = []
        
        
        
    def init(self):
        # 测试目标实例是否可以正常工作。
        logger.info('sap big table initialing the sap system :{0}'.format(self.__DestName))
        
        self.__Dest = sapint.GetDestination(self.__DestName)
        
        
    def AddCriteria(self, SQL):
        if len(SQL) > 71:
            raise SAPException('SQL too long')
        else:
            self.__Options.append(SQL)
        
    def AddField(self, field):
        self.__FieldsIn.append(field)
    
    def __addWhereLine(self, toptions, whereline):
        toptions.appendRow()
        toptions.setValue("TEXT", whereline)
        
    def submit(self):
        
        t1 = None
        t2 = None

        logger.info ("Begin to submit sap table {0} ...........".format(self.TableName))
        logger.info ('Read table function is:{0}'.format(self.FunctionName))
        
        if self.TableName == '':
            raise SAPException('Tablename is need!!')
    
#         try:
        __function = object
        self.__FetchedRows = 0
        if self.RowCount == 0:
            self.RowCount = 299000000
        __fd = self.__Dest.discover("ZVI_BIG_TABLE_SUBMIT")
        __function = __fd.create_function_call()

        logger.info ("TableName:{0},Rows:{1},Skip:{2},Delimeter:{3}".format(self.TableName, str(self.RowCount), str(self.RowSkip), self.Delimiter))
        logger.info ("Table Options.:" + str(self.__Options))
        logger.info ("Table Fields.:" + str(self.__FieldsIn))
        
        __function.QUERY_TABLE(self.TableName)
        __function.ROWCOUNT(self.RowCount)
        __function.ROWSKIPS(self.RowSkip)
        if self.Delimiter != None and self.Delimiter != '':
            __function.DELIMITER(self.Delimiter)
        __function.FLINES(self.FileLines)

        __opts = []
        for op in self.__Options:
            __optl = {}
            if op != '':
                # logger.info op
                __optl["TEXT"] = op
                __opts.append(__optl)

        __function.OPTIONS(__opts);
        
        __fields = []
        for f in self.__FieldsIn:
            __field = {}
            if f != '':
                __field["FIELDNAME"] = f
                __fields.append(__field)
        __function.FIELDS(__fields)
        t1 = datetime.now()
        logger.info('RFC function {0} invoked beginned...........'.format(self.FunctionName))
        
        __function.invoke()
        
        t2 = datetime.now()
        __uuid = __function.E_UUID.value;
        logger.info('Big table {0} submit...........'.format(self.TableName))
        __return = __function.RETURN.value;
        __ret = sapint.SharedFunction.StripRfcTable(__return)
        for ret in __ret:
            if ret['TYPE'] == 'E':
                raise SAPException(ret['MESSAGE'])
        return __uuid
    
       
        
    def search(self, pUUID, pTable, pName, pDate, pCode):
        
        __function = object
        __fd = self.__Dest.discover("ZVI_BIG_TABLE_SEARCH")
        __function = __fd.create_function_call()
        # __function = self.__Dest.getRepository().getFunction("ZVI_BIG_TABLE_SEARCH")
        
        __function.I_DATE(pDate)
        __function.I_UNAME(pName)
        __function.I_UUID(pUUID)
        __function.I_TABLE(pTable)
        __function.I_CODE(pCode)
        
        
        __function.invoke()
        
        __rfcheader = __function.ET_HEADER.value
        __rfcdetail = __function.ET_DETAIL.value
        __return = __function.RETURN.value
#         print(__return)
        # __ret = sapint.SharedFunction.StripRfcTable(__return)
        for ret in __return:
            if ret['TYPE'] == 'E':
                raise SAPException(ret['MESSAGE'])
        

        out = {}
        out['header'] = sapint.SharedFunction.StripRfcTable(__rfcheader)
        out['detail'] = sapint.SharedFunction.StripRfcTable(__rfcdetail)
        
#             out['header'] = sapint.SharedFunction.EscapeList(__header)
#             out['detail'] = sapint.SharedFunction.EscapeList(__detail)
        return out
        

    def read(self, pUUID, pPos):
#         out = {}


        __function = object
        __fd = self.__Dest.discover("ZVI_BIG_TABLE_READ")
        __function = __fd.create_function_call()

        __function.I_UUID(pUUID)
        __function.I_POSNR(pPos)


        __function.invoke()
        
        __header = __function.E_HEADER.value
        self.TableName = __header["TBLNAME"].strip()
        self.Delimiter = __header["DELIMITER"].strip()
        
        
        __return = __function.ET_RETURN.value
        __ret = sapint.SharedFunction.StripRfcTable(__return)
        for ret in __ret:
            if ret['TYPE'] == 'E':
                raise SAPException(ret['MESSAGE'])
        self.Return = __ret
        
        __table = CReadTable('')
        __table.Delimiter = self.Delimiter
        logger.info("the delimiter is {0}".format(__table.Delimiter))
        __table.processRetriveData(__function)
        
#            return the table instance ,because there are other useful function in the instance
        return __table
    
#             out['fields'] = __table.GetFields()
#             out['data'] = __table.GetResult()

#             out['header'] = sapint.SharedFunction.EscapeList(__header)
#             out['detail'] = sapint.SharedFunction.EscapeList(__detail)
#             return out
        

        
    def delete(self, pUUID, pPos, pDeleteAll, pReset):
        out = {}
        logger.info('try to delete sap table file uuid is {0}'.format(pUUID))
        
        
        __function = object
        
        __fd = self.__Dest.discover("ZVI_BIG_TABLE_DELETE")
        __function = __fd.create_function_call()
        __function.I_UUID(pUUID)
        __function.I_POSNR(pPos)
        __function.I_DELALL(pDeleteAll)
        __function.I_RESET(pReset)


        __function.invoke()
        
        __rc = __function.E_RC.value
        __return = __function.ET_RETURN.value
        out['rc'] = __rc
        out['return'] = sapint.SharedFunction.StripRfcTable(__return)
        
        return out
    

if __name__ == "__main__":
    pass
#     print 'class CBigTable'
    bigTable = CBigTable("AIP")
# #     
# # # test submit function
#     bigTable.TableName = 'MARA'
#     bigTable.RowCount = 20
#     uuid = bigTable.submit()
#     print(uuid)

# test search
    ret = bigTable.search('', '', '', '', '')
    logger.info(ret)
#     print(ret)
#     ret2 = json.dumps(ret)
#     print(ret2)
#     ret2 = json.dumps(ret).decode('unicode-escape')
#     print(ret2)
#         

# test read function 
    ret = bigTable.read('00E081D518961ED3B1EC49BCE815A4DAMARA', 1)
    logger.info(ret.GetResultAsDict())
      
    
    
# test the delete function
#     ret = bigTable.delete('00E081D518961ED3AF8B2A9D2C3724DA', '1', '', '')
#     ret2 = json.dumps(ret).decode('unicode-escape')
#     print ret2
