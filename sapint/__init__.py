# -*- coding: utf-8 -*-
'''
Created on 2014-4-18

@author: wangweisheng
'''
__all__ = ['SharedFunction','SAPException']

import sys
import sapnwrfc


import yaml
import logging

from sapnwrfc import RFCException
from sapnwrfc import RFCCommunicationError
from sapnwrfc import RFCServerError
from sapnwrfc import RFCFunctionCallError


BASE_CONFIG_DIR = "c:\\temp\\"
CONFIG_FILE = ""
SAP_CONFIG_FILE_YAML = "d:\\sapconfig.yml"
SAP_LOG_FILE = "D:\\sappyrfc.log"
logger = None

def getCommentLogger(pName):
#     logger = logging.getLogger(pName)
    logger = logging.getLogger(pName)
    logger.setLevel(logging.DEBUG)
    if not logger.handlers:
        fh = logging.FileHandler(SAP_LOG_FILE,encoding = "UTF-8")
        fh.setLevel(logging.DEBUG)
        ch = logging.StreamHandler()
        ch.setLevel(logging.DEBUG)
        formmatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
        fh.setFormatter(formmatter)
        ch.setFormatter(formmatter)
        logger.addHandler(ch)
        logger.addHandler(fh)
    return logger

logger = getCommentLogger(__name__)


class SAPException(Exception):
    """ 定义一个SAP出错     """
    def __init__(self,value):
        self.value = value
    def __str__(self):
        return repr(self.value)


def GetDestination(destName):
    if destName == None or destName == '':
        raise SAPException('SAP system name missed')
    try:
        #CONFIG_FILE = BASE_CONFIG_DIR + destName + ".yml"
        #sapnwrfc.base.config_location = CONFIG_FILE
        #sapnwrfc.base.load_config()
        #conn = sapnwrfc.base.rfc_connect()
        cfg = load_client_config(destName.upper())
        conn = sapnwrfc.base.rfc_connect(cfg)
        #logger.info(conn.connection_attributes())
        return conn
    #except RFCCommunicationError as x:
        
    except Exception as ex:
        template = "An exception of type {0} occured. Arguments:\n{1!r}"
        message = template.format(type(ex).__name__, ex.args)
             
        logger.error(message)
        raise SAPException(message)

def load_client_config(destName):
    configuration = yaml.load(open(SAP_CONFIG_FILE_YAML, 'rb').read())
    #print(yaml.dump(configuration))
    cfg = configuration[destName]['client']
    return cfg

def get_sap_client_list():
    systemlist  = {}
    configuration = yaml.load(open(SAP_CONFIG_FILE_YAML, 'rb').read())
    for k,v in configuration.items():
#         print(k,v)
        systemlist[k] = v['desc']
    return systemlist
    
def CheckSap(destName):
    destination = GetDestination(destName)
    if destination == None:
        logger.error(destName + "is not available,now returning..........")
        return False
    
    
if __name__ == "__main__":
    pass
#     print("test")
    con = GetDestination("AIP")
    
    con.close()

    print(get_sap_client_list())