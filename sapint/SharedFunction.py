# -*- coding: utf-8 -*-
'''
Created on 2014-4-18

@author: wangweisheng
'''
import sys
# print sys.path
import json
import itertools
import datetime
# from datetime import *

import sapint
logger = sapint.getCommentLogger(__name__)
# reload(sys)
# sys.setdefaultencoding('utf8') 

# 返回RFC函数的抬头字段列表。
def GetRfcTableHeader(fieldtab):
	logger.info("GetRfcTableHeader")

	headers = [e.decode().strip() for e in fieldtab.keys]
	return headers



# 把rfctable转换成jython List,根据RFC表的列数，动态生成一个JSON数组
def StripRfcTable(fieldtab):
	logger.info("RfcTableToList decode RFC table to python list")
	rows = []
	
	for fline in fieldtab:
		__lined = {}
		for field in fline:
			if not type(fline[field]) == int:
				__lined[field.strip()] = fline[field].rstrip()
			else:
				__lined[field.strip()] = fline[field]
		rows.append(__lined)
	return rows
#     # print("Table Rows Count: " + fieldtab.getNumRows())
#     types = fieldtab.getMetaData()
#     for r in range(fieldtab.getNumRows()):
#         fieldtab.setRow(r)
#         row = []
#         
#         for c in range(fieldtab.getFieldCount()):
#             # 不应该在这里处理RFC返回的数据，因为这个LIST保存了最原始的信息。
#             # print fieldtab.getValue(c)
#             # value = fieldtab.getValue(c)
# #             if '"' in value:
# #                 print value
# #                 value = value.replace('"','\\"')
# #                 print value
#             
#             t = types.getType(c)
# #             print 'rfc type is ', t
#             v = fieldtab.getValue(c)
#             nv = None
#             if t == 0:
#                 nv = str(v)
#             elif t == 2:
#                 nv = int(v)
#             elif t == 1:
#                 if v == '00000000':
#                     nv = None
#                 else:
#                     nv = date(int(v[0:4]), int(v[4:6]), int(v[6:8]))
#             elif t == 3:
#                 if v == '000000':
#                     nv = None
#                 else:
#                     nv = time(int(v[0:2]), int(v[2:4]), int(v[4:6]))
#             elif t == 6:
#                 nv = int(v)
#             elif t == 7:
#                 nv = float(v)
#             else:
#                 nv = v
#             row.append(nv)
#         rows.append(row)
#     return rows

# 合并抬头与内容,并返回新的LIST
def CombineHeaderAndContent(headers, list):
	logger.info("CombineHeaderAndContent to JSON object")

	newlist = EscapeList(list)
	newheader = headers
#     for element in list:
#             row = []
#             for ele in element:
#                 if '\\' in ele:
#                     ele = ele.replace('\\','\\\\')
#                     print ele
#                 if '"' in ele:
#                     ele = ele.replace('"','\\"')
#                     print ele
#                 row.append(ele)
#             newlist.append(row)
#             

	dicList = []
	row = {}
	result = ""
	#     try:

	for element in newlist:
#         print(element)
#         print(newheader)
		# row = dict(zip(headers,element))
		row = dict(zip(newheader, element))
#             print 'combined row',row
		newrow = {}
#         print(row)
		for k,v in row.items():
			if v != None:
				newrow[k] = v
#         for k in row.it:
#             if row[k] != None:
#                 newrow[k] = row[k]

#             print 'newrow ',newrow
		dicList.append(newrow)
		del newrow
	#         result = json.dumps(dicList)
	#         result = result.decode('unicode-escape')
	#     except:
	#         logger.error(sys.exc_info())
	#         # result = list
	#         result = []
	del row
	return dicList


# 根据传入的LIST，把它转换成JS中JSON格式的数组。这个方法返回的数据比ListToJson返回的数据在体积上要小，也更快。
# 但应用方便并不是特别的广，因为很多的JS库并没有针对数组操作的处理。
def EscapeList(plist):
	if isinstance(plist, list) == False:
		logger.error('Error .. not list')
		return None

	logger.info("EscapeList: list length is :" + str(len(plist)))
	result = ""
	newlist = []
	# 在返回的结果中可能会包含JSON 不兼容的字符，所构造一个新的列表。
	#     try:
	
	nele = None
	for element in plist:
		if isinstance(element, list) == False:
			logger.error('Error .. not list')
			break
		row = []
		for ele in element:
			nele = ele
			if ele != None:
				if isinstance(ele, datetime.date):
					if len(str(ele)) == 8:
						nele = ele.strftime('%Y-%m-%d')
					else:
						nele = str(ele)
#                         print 'converted type is date: {0}'.format(nele)
				elif isinstance(ele, datetime.time):
					if len(str(ele)) == 8:
						nele = ele.strftime('%H:%M:%S')
					else:
						nele = str(ele)
#                         print 'converted type is time: {0}'.format(nele)
				elif isinstance(ele, int):
#                         print 'ele is numberd'
					nele = int(ele)
				elif isinstance(ele, bytes):
					return nele
				elif isinstance(ele, str):
					if '\\' in ele:
						nele = ele.replace('\\', '\\\\')
#                             print nele
					if '"' in ele:
						nele = ele.replace('"', '\\"')
#                             print '\\\\ meet',nele
#                             print nele
#                     else:
#                         print 'not converted',ele
			row.append(nele)

		newlist.append(row)

#         result = json.dumps(newlist)
#         #这样才可以反解析出正确的中文
#         print 'Finished Convert python list to Json Array'
#         result = result.decode('unicode-escape')
#         print result
#         print "解析结束"
#     except:
#         logger.error(sys.exc_info()) 
#         raise Exception
		# result = list

	return newlist

if __name__ == "__main__":
	pass
#     print EscapeList([[u'测"试']])