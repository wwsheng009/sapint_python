# -*- coding: utf-8 -*-
import sys


from sapint.db.SapTableToMysql import SapTableToMysql

def get_condtion(pAlpha):
    return """SPRSL = '1'""".format(pAlpha)
def copytable(pDest): 
    
    sapmysql = SapTableToMysql()
    sapmysql.sapClient = pDest
    
    sapmysql.tableName = 'MAKT'
    try:
        sapmysql.CopySAPTable([], [], 100, True, False)
    except Exception as e:
        print (e)
        print (sys.exc_info())

if __name__ == "__main__":
    copytable('AIP')
