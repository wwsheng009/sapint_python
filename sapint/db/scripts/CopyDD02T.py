import sys
from sapint.db.SapTableToMysql import SapTableToMysql
from sapint.db.SapTableToSqlite import SapTableToSqlite


from collections import defaultdict
from gc import get_objects
before=defaultdict(int)
after=defaultdict(int)
#for i in get_objects():before[type(i)]+=1:
	#print [(k,after[k]-before[k]) for k in after if after[k]-before[k]]



#from sapint.db.SapTableToOracle import SapTableToOracle

# 按字母排序倒出SAP数据库表中的数据

def get_condtion(pAlpha):
	return """DDLANGUAGE = '1' and tabname like '{0}%'""".format(pAlpha)

def copy_dd02t(pDest):

#     sapmysql = SapTableToMysql()
#     sapmysql = SapTableToSqlite()
	sapmysql = SapTableToSqlite()
	sapmysql.sapClient = pDest

	sapmysql.tableName = 'DD02T'

	try:
		for i in get_objects():before[type(i)]+=1
		sapmysql.CopySAPTable([], [get_condtion('A')], 0, False, True)
		for i in get_objects():after[type(i)]+=1
		print ([(k,after[k]-before[k]) for k in after if after[k]-before[k]])
		objts = gc.get_objects()
		print(objts)
		#for alpha in ['B','C','D','E','F','G','H','I','J','K','L','M','N','O','P','Q','R','S','T','U','V','W','Y','Z']:

			#sapmysql.CopySAPTable([], [get_condtion(alpha)], 0, False, True)
	except Exception as e:
		print (e)
		print (sys.exc_info())

if __name__ == "__main__":
	copy_dd02t('AIP')