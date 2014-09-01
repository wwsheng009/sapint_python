# -*- coding: utf-8 -*-
'''
Created on 2014-4-18

@author: wangweisheng
'''

import sys
import sapint
from sapint import SAPException

logger = sapint.getCommentLogger(__name__)

from sapint import SharedFunction
class CReadTable(object):
	
	'''
	classdocs
	'''
	Delimiter = ''
	__FetchedRows = 0
	__FieldsOut = []
	__FieldsOutDict = []
	__FieldsIn = []
	__Options = []

	FunctionName = 'ZVI_RFC_READ_TABLE'
	#read 1 row record default
	RowCount = 1
	RowSkip = 0
	NoData = ''
	TableName = ''
	__WhereClause = ''

	__DestName = ''
	__Dest = object

	Result = []

	DATEFORMAT = "{0}.{1}.{2}"
	TIMEFORMAT = "{0}:{1}:{2}"

	def __init__(self, pSapSystem):
		'''
		Constructor
		'''
		self.__FieldsIn = []
		self.__Options = []
		self.__FieldsOut = []
		self.Delimiter = ''
		#self.RowCount = 0 
		self.RowSkip = 0
		self.NoData = ''
		self.__DestName = pSapSystem

		if self.__DestName != None and self.__DestName != '':
			self.init()

		self.__tableDef = []
	def __del__(self):
		pass
		#del self.Result
		#del self.__FieldsOutDict
		#del self.__FieldsIn
		#del self.__Options
		#del self.__FieldsOut
		#del self.__tableDef

	def clearAll(self):
		for i in self.Result:
			i = None
		for i in self.__FieldsOutDict:
			i = None
		self.Result = None
		self.__FieldsOutDict = None

	def init(self):
		# 测试目标实例是否可以正常工作。
		logger.info('check if the sap {0} is running'.format(self.__DestName))
		try:
#             if sapint.CheckSap(self.__DestName) == False:
#                 raise SAPException('SAP system ' + self.__DestName + 'is not available')
			self.__Dest = sapint.GetDestination(self.__DestName)
		except NotImplementedError as error:
			logger.error(sys.exc_info())
			raise(error)

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

	# #直接返回表结果
	def GetResult(self):
		return self.Result
	# #直接返回字段列表
	def GetFields(self):
		return self.__FieldsOutDict

	def GetFieldsFull(self):
		return self.__tableDef
	def GetResultAsDict(self):
		logger.info('call methond GetResultAsJson()')
		header = [x['FIELDNAME'] for x in self.__FieldsOutDict]
		return sapint.SharedFunction.CombineHeaderAndContent(header, self.Result)

	def Run(self):

#         t1 = None
#         t2 = None

		logger.info ("Begin to read sap table {0} ...........".format(self.TableName))
		logger.info ('Read table function is:{0}'.format(self.FunctionName))
#         try:
		__function = object
		# __table1 = object
		# __table2 = object

		self.__FetchedRows = 0
		if self.RowCount == 0:
			self.RowCount = 299000000 
		__fd = self.__Dest.discover(self.FunctionName)

		__function = __fd.create_function_call()

		logger.info ("TableName:{0},Rows:{1},Skip:{2},Delimeter:{3}".format(self.TableName, str(self.RowCount), str(self.RowSkip), self.Delimiter))
		logger.info ("Table Options.:" + str(self.__Options))
		logger.info ("Table Fields.:" + str(self.__FieldsIn))

		__function.QUERY_TABLE(self.TableName)
		__function.ROWCOUNT(self.RowCount)
		__function.ROWSKIPS(self.RowSkip)
		__function.DELIMITER(self.Delimiter)
		__function.NO_DATA(self.NoData)

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
		# 开始调用
#         t1 = datetime.now()
		logger.info('RFC function {0} invoked beginned...........'.format(self.FunctionName))

		__function.invoke()

#         t2 = datetime.now()
#             logger.info('rfc function invoked end.','Duration is:{0}'.format(t2 - t1))


		self.processRetriveData(__function)
		self.__Dest.close()

#             __table6 = __function.getTableParameterList().getTable("ET_FIELDS");
#             self.__tableDef = sapint.RfcTableToList(__table6)
#             logger.info 'table definition ',self.__tableDef


		# 小心不要在结构处理之前清除结构，因为它们的引用对象一样
		# __table1.clear()
		# __table2.clear()
#         except:
#             logger.error(sys.exc_info())
#             raise SAPException(sys.exc_info())

	def processRetriveData(self, pfunction):

		# 返回的FIELD清单

		logger.info ('Process the RetriveData..and Converting data...')
		__table3 = pfunction.FIELDS.value;
		__table4 = pfunction.DATA.value;

		logger.info ('Field count: {0} ,data rows: {1}'.format(len(__table3), len(__table4)))

		#__fields = []
		#for fline in __table3:
##                 __line = {}
			#__line = []
			#for f in fline:
##                     print(__line)
##                     __line[f.decode().strip()] = fline[f].decode().strip()
				#v = fline[f].strip()
				#__line.append(v)
			#__fields.append(__line)
##             print(__fields)
		#self.__FieldsOut = __fields

		__fields = []
		
		for fline in __table3:
			__lined = {}
			for f in fline:
				__lined[f.strip()] = fline[f].strip()
			__fields.append(__lined)
			#del __lined
#             print(__fields)
		self.__FieldsOutDict = __fields


		
		
		line = None
		__table = []
		for data in __table4:
			line = data['FELD']
			__row = []
			
			if self.Delimiter == '' or self.Delimiter == None:
				for field in self.__FieldsOutDict:
#                         print('field:',field)
					fname = field['FIELDNAME'].strip()
					offset = int(field['OFFSET'])
					length = int(field['LENGTH'])
					type = field['TYPE'].strip()
#                         print('fieldname',fname,'offset',offset,'length',length,'type',type)
					d = line[offset: offset + length].strip()
					d = self.determineValueByType(d, fname, type, length)

#                        __row[fname] = (d)
					__row.append(d)
			else:
				__r = line.split(self.Delimiter)
				__row = [e.strip() for e in __r]
				__index = 0

				for _findx in range(len(self.__FieldsOutDict)):
					field = self.__FieldsOutDict[_findx]
					fname = field['FIELDNAME'].strip()
					offset = int(field['OFFSET'])
					length = int(field['LENGTH'])
					vtype = field['TYPE'].strip()
					__row[_findx] = self.determineValueByType(__row[_findx], fname, vtype, length)

#                      for _findx in range(len(self.__FieldsOut)):
# #                     for f in self.__FieldsOut:
#                         f = self.__FieldsOut[_findx]
# #                         print(f)
# #                         logger.info f
#                         __type = f[4].strip()
#                         __fieldName = f[0].strip()
#                         __row[_findx] = self.determineValueByType(__row[_findx], __type, __fieldName)
#                         
			__table.append(__row)

#             print(__table)
		self.Result = __table

		__table6 = pfunction.ET_FIELDS.value
		self.__tableDef = sapint.SharedFunction.StripRfcTable(__table6) 

#             print(self.__tableDef)
		logger.info ('Converting data finished')
	def determineValueByType(self, pInput, pField, pType, pLength):
		#print('field: {3},data is :{0} type {1},length is {2}'.format(pInput,pType,pLength,pField))
		try:

			d = pInput
			pType = pType.upper()
			o = None

			if pType == 'P' or pType == 'F':            
#                 if d == '' or d == None or d.find(' '):
#                     o = None
	#             logger.info 'Convert double or float data...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
				if d[-1] == '-':
	#                 logger.info d
					d = d.rstrip('-')
					d = '-' + d
	#                 logger.info d
					o = float(d)
				else:
					o = float(d)
			elif pType == 'D':
	#             logger.info 'Convert DATE ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
	#             if d != '' or d != None:
	#                 d = d.ljust(8,'0')
#                 if len(d) != 8 or (d != '' or d != None):
#                     return None
#                     raise Exception('Exception date:{0} is not valid'.format(d))      
	#             logger.info d

				if d == '00000000':
					pass
				elif d == '':
					pass
				else:
#                     o = date(int(d[0:4]), int(d[4:6]), int(d[6:8]))
					o = self.DATEFORMAT.format(d[0:4], d[4:6], d[6:8])
			elif pType == 'T':
	#             logger.info 'Convert TIME ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
				if len(d) != 6 and (d != '' and d != None):
					return None
#                     raise Exception(' Exception time:{0} is not valid'.format(d))
	#             if d != '' or d != None:
	#                 d = d.ljust(6,'0')
				if d == '000000':
					pass
				elif d == '':
					pass
				else:
#                     o = time(int(d[0:2]), int(d[2:4]), int(d[4:6]))
					o = self.TIMEFORMAT.format(d[0:2], d[2:4], d[4:6])
			elif pType == 'N':
	#             logger.info 'Convert N ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
#                 if d != '' and d != None:
				o = d
			elif pType == 'B':
				o = d           
			elif pType == 'b':
	#             logger.info 'Convert b ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)

				if d != '' and d != None :
					o = int(d)
				else:
					o = 0
			elif pType == 'S':
	#             logger.info 'Convert b ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)

				if d != '' and d != None :
					o = int(d)
				else:
					o = 0            
			elif pType == 's':
				if d == '' or d == None or d.find(' '):
					o = 0
	#             logger.info 'Convert s ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
				if d[-1] == '-':
	#                 logger.info d
					d = d.rstrip('-')
					d = '-' + d
	#                 logger.info d
					o = int(d)
				else:
					o = int(d)
#                 if d != '' and d != None and d != 0:
#                     return int(d)
			elif pType == 'C':
	#             logger.info 'Convert CHAR ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
				if d == '':
					return None
				else:
					o = d
			elif pType == 'I':
	#             logger.info 'Convert I ...field:{0},type:{1},value:{2}'.format(pField,pType,pInput)
				if d[-1] == '-':
	#                 logger.info d
					d = d.rstrip('-')
					d = '-' + d
	#                 logger.info d
					o = int(d)
				else:
					o = int(d)
#                 if d != '' and d != None and d != 0:
#                     return int(d)
			elif pType == 'x':
				o = d
			else:
				logger.info ('Data Not converted ...field:{0},type:{1},value:{2}'.format(pField, pType, pInput))
				o = d
#             print('converted data is ', o)
			return o
		except :
			raise Exception("Can't convert data...field:{0},type:{1},value:{2}".format(pField, pType, pInput))


if __name__ == "__main__":
	pass
	#table = CReadTable("EYANGDEV")
	#table.TableName = "MAKT"
	#table.RowCount = 2

#     table.AddCriteria("MATNR > ''")
	#table.Run()
	#logger.info(table.GetFields())
#     logger.info(table.GetResultAsDict())

#     logger.info(table.GetFieldsFull())
	#result = table.GetResult()
	#logger.info(result)
#     for x  in range(len(result)):
#         for y in range(len(result[x])):
#             print(x, y, result[x][y]);
