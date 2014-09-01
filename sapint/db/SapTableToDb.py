import sys

# print sys.getdefaultencoding()
import sapint
from sapint.tableutil.ReadTable import CReadTable
from datetime import *

import json
logger = sapint.getCommentLogger(__name__)

class SapTableToDb():
	def __init__(self):
		self.schema = ''
		self.tableName = ''
		self.sapClient = ''
		self.sapServer = ''

		self.fieldsIn = []
		self.fieldsOut = []
		self.tableDef = []
		self.dataOut = []

		self.Delimeter = ''
		self.NoData = ''


		self.insertSQL = ''
		self.createSQL = ''
		self.dropSQL = ''
		self.cleanSQL = ''

		self.createTableType = '1'

		self.conn = None
		self.getDbConnection()

	def CopySAPTable(self,pFields, pOption, pRows,pForce,pAppend):
		"""复制SAP表到MYSQL数据库，在读取表的字段清单时使用RFC 函数DDIF_FIELDINFO_GET
		字段的信息会更完整，但是速度比较慢
		pDestName ,SAP连接
		pTable,表名
		pFields,字段列表
		pOption,条件列表
		pRows,字段条数限制
		pDelimmiter,分隔符
		pForce,重建数据库表
		pAppend,只附加数据，并不是清空
		"""		
		if self.sapClient == None :
			raise Exception('Sap Client is missing')
		elif self.sapClient == '':
			raise Exception('Sap client is required')

		if self.tableName == None :
			raise Exception('TableName is required')
		else:
			if self.tableName == '':
				raise Exception('TableName is required')

		logger.info('Begin to copy table {0} from sap {1}'.format(self.tableName,self.sapClient))
		self.readTableContent(pFields, pOption, pRows)
		tableExist = self.checkTableExist()
		if pForce == True or tableExist == False:
			self.dropTable()
			self.createTable()
		else:
			if pAppend !=True:
				self.cleanTable()

		self.insertData()


	def PushDataBySAP(self,pTable,pFields,pData,pTableDef,pForce,pAppend):
		'''pTable,表名
		pFields,字段清单
		pData,数据
		pTableDef,SAP表的结构定义
		pForce,强制建表
		pAppend,附加数据到数据库表中
		'''
		self.tableName = pTable
		self.dataOut = pData
		self.fieldsOut = pFields
		self.tableDef = pTableDef

		logger.info('table {0} pushed by sap'.format(self.tableName))
	#     read the table fields info
		tableExist = self.checkTableExist()
		if self.tableDef == [] and tableExist == False:
			raise Exception('the table is not exist and tablestruct is empty')

		if pForce == True or tableExist == False:
			self.dropTable()
			self.createTable()
		else:
			if pAppend !=True:
				self.cleanTable()

		self.insertData()

	def batchCopy(self,pDestName,pTableList,pDelimiter):
		for t in pTableList:
	#         CopySAPTable(pDestName, t,[],[],1000,pDelimiter,True)
	#         CopySAPTable(pDestName, t,[],[],None,pDelimiter,True)
	#         CopySAPTable('AIP', 'mara',[],[],10,None,True)
			CopySAPTable('AIP', 'marc',[],[],10,True,True)
	def readTableFields(self):
		try:
			table = CReadTable(self.sapClient)
			table.TableName = self.tableName
			self.tableDef = table.GetFieldInfo()

		except Exception as e :
	#         traceback.print_exc()
			logger.error(str(e))
			logger.error(sys.exc_info())
			raise e
			# return sys.exc_info() 

	def readTableContent(self, pFields, pOption, pRows):
#         result = {}
		self.fieldsIn = pFields
		try:
			table = CReadTable(self.sapClient)
			table.TableName = self.tableName
			table.Delimiter = self.Delimeter
			table.RowCount = pRows
			table.NoData = self.NoData

			table.FunctionName = 'ZVI_RFC_READ_TABLE'

			for f in pFields:
				table.AddField(f)
			for o in pOption:
				table.AddCriteria(o)

			table.Run();

			self.fieldsOut = table.GetFields()
			self.dataOut = table.GetResult()
			self.tableDef = table.GetFieldsFull()
			logger.info( 'table definition :')
#             logger.info(str(self.tableDef))

		except Exception  as e:
			logger.error(sys.exc_info())
			logger.error(str(e))
			raise e


	def execute(self,stmt):
		"""执行SQL语句，并返回第一行数据"""
		logger.info('execute sql statment: ' + stmt)
		ret = None
#         conn = self.getDbConnection()
#         if self.conn == None:
		self.conn = self.getDbConnection()
		conn = self.conn

		#with conn:
		c = conn.cursor()

		try:
			c.execute(stmt)
			ret = c.fetchone()

			#for result in c.execute(stmt):
				#if result.with_rows:
				#print("Statement '{}' has following rows:".format(
				#result.statement))
				#print(result.fetchall())
				#else:
				#print("Affected row(s) by query '{}' was {}".format(
				#result.statement, result.rowcount))			

			#if c.rowcount > 0:
				#ret = c.fetchone()
		except Exception as msg:
			c.close()
			conn.rollback()
			conn.close()
			logger.info( msg)
			raise msg

		#conn.commit()
		conn.close()
		return ret

	def prepareLineData(self,data):
		return data

	def insertData(self):
		"""
		            往数据库插入内表数据，在连接MYSQL数据库时，一定要把参数useUnicode=yes&characterEncoding=UTF-8加入，
		            否则插入中文时会出否乱码
		"""
		if self.dataOut == None: 
			logger.warning('Nothing to insert')
#             sys.exit(0)
		if self.dataOut == []:
			logger.warning('Nothing to insert')
#             sys.exit(0)

		self.getInsertStatment()
		if self.insertSQL == '':
			logger.error('Failed to crate insertSQL')
			raise Exception('Failed to crate insertSQL')
#             sys.exit(0)
		conn = self.getDbConnection()
		c = conn.cursor()
		#with conn:
			#with conn.cursor() as c:
		for d in self.dataOut:
			try:
				d = self.prepareLineData(d)
				c.execute(self.insertSQL, d)
				#在这里不要使用executemany 插入整个数组，因为磁盘IO可能反应不过来
				#c.executemany(self.insertSQL, d.dataOut)
			except Exception  as e:
				c.close()
				logger.info('Inserting data is ........')
				logger.error(sys.exc_info())
				conn.rollback()
				conn.close()
				logger.error(str(e))
				raise e

		conn.commit()
		conn.close()
		logger.info('Insert compelete!')


	def selectAllData(self):
		"""读取所有的表数据"""
		conn = self.getDbConnection()
		stmt = self.getSelectAllStatument()
		#with conn:
			#with conn.cursor() as c:
		c = conn.cursor()
		try:
			c.execute(stmt)
			return c.fetchall()
			#if c.rowcount > 0:
				#logger.info('rows:'.format(c.rowcount))
			#else:
				#logger.info('no rows selected')

		except Exception as msg:
			conn.rollback()
			conn.close()
			logger.error(str(msg))
			logger.error(sys.exc_info())

		conn.commit()
		conn.close()



	def checkTableExist(self):
		stmt = self.getCheckExistStatment()
		"""SQLITE 检查数据库表是否已经存在"""
		print (datetime.now(),'check if the table {0} is exist'.format(self.tableName))
		re = self.execute(stmt)
		if re is None:
			logger.warning('{0} is not exist'.format(self.tableName))
			return False
		else:
			logger.info( '{0} is exist'.format(self.tableName))
			return True

	def dropTable(self):
		if self.checkTableExist() == True:
			stmt = self.getDropStatment()
			self.execute(stmt)
			logger.warning('Table:{0} dropped!!'.format(self.tableName))

	def cleanTable(self):
		if self.checkTableExist() == False:
			return
		stmt = self.getCleanStatment()
		self.execute(stmt)
		logger.warning('Table:{0} cleanned!!'.format(self.tableName))

	def createTable(self):
		createSQL = self.getCreateStatment2()
		logger.info(createSQL)
		self.execute(createSQL)
		logger.warning('Table:{0} created!!'.format(self.tableName))




	def getInsertStatment(self):
		return None

	def getInsertStatment2(self):
		"""根据DDIF_FIELDINFO_GET返回的字段清单，拼接SQL 语句
		        只适合整个表复制的情况，因为返回的值可能只有部分字段
		"""
		return None

	def getDbConnection(self):
		pass
	def getSelectAllStatument(self):
		return 'select * from {0}'.format(self.tableName)
	def getCreateStatment(self):
		pass
	def getCreateStatment2(self):
		pass
	def getDropStatment(self):
		pass
	def getCheckExistStatment(self):
		pass
	def getCleanStatment(self):
		pass

if __name__ == '__main__':
	print ('script SapTableToDb loaded and Runned')