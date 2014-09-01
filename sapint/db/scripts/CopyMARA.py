# -*- coding: utf-8 -*-
import sys


from sapint.db.SapTableToHana import SapTableToHana
from sapint.db.SapTableToMysql import SapTableToMysql

def get_condtion(pAlpha):
    return """SPRSL = '1' and TCODE like '{0}%'""".format(pAlpha)
def copy_dd02t(pDest): 
    
    sapmysql = SapTableToHana()
    sapmysql.sapClient = pDest
    sapmysql.schema = "VITEST"
    sapmysql.tableName = 'MARA'
    try:
        sapmysql.CopySAPTable([], [], 100, True, False)
    except Exception as e:
        print (e)
        print (sys.exc_info())

if __name__ == "__main__":
    copy_dd02t('AIP')
