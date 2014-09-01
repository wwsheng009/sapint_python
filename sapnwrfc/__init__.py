
"""
Python utils for RFC calls to SAP NetWeaver System
"""

import sys
if sys.version < '2.4':
    print('Wrong Python Version (must be >=2.4) !!!')
    sys.exit(1)

# load the native extensions
import nwsaprfcutil

import sapnwrfc.rfc

from struct import *
from string import *
import re
from types import *
#from copy import deepcopy



# Parameter types
IMPORT = 1
EXPORT = 2
CHANGING = 3
TABLES = 7

CONFIG_OK = ('ashost', 'sysnr', 'client', 'lang', 'user', 'passwd', 'gwhost', 'gwserv', 'tpname', 'lcheck','saprouter')


CONF_FILE = 'sap.yml'

class RFCException(Exception):
    def __init__(self, value):
        self.value = value

    def __str__(self):
        return repr(self.value)


class base(object):
    """
    Base class used to trigger everything off
    """

    config_location = CONF_FILE
    configuration = {}

    @classmethod
    def load_config(cls):
        # REFACTOR: there is no need to depend on yaml
        import yaml

        cls.configuration = yaml.load(open(cls.config_location, 'rb').read())
        #cls.configuration = yaml.load(open(cls.config_location, 'rb').read())
        return cls.configuration

    @classmethod
    def rfc_connect(cls, cfg=None):
        config = {}
        # pass in the config load from config_location YAML file
        for k, v in cls.configuration.items():
            if k in CONFIG_OK:
                if not k in ('gwserv', 'gwhost', 'tpname', 'loglevel'):
                    config[k] = str(v)
        # Overload the YAML file config with parameters passed to 
        # rfc_connect
        if not cfg == None:
            if not type(cfg) == dict:
                raise RFCException("Config passed to rfc_connect must be a Dictionary object")
            for k, v in cfg.items():
                if k in CONFIG_OK:
                    if not k in ('gwserv', 'gwhost', 'tpname', 'loglevel'):
                        config[k] = str(v)
        #conn = sapnwrfcconn.new_connection(config)
        conn = nwsaprfcutil.Conn(config)
        c = connection(conn)
        return c


class connection:
    """
    Connection class - must not be created by the user - automatically generated by 
    a call to sapnwrfc.base.rfc_connect()
    """

    def __init__(self, handle=None):
        self.handle = handle

    def connection_attributes(self):
        if self.handle == None:
            raise RFCException("Invalid handle (connection_attributes)\n")
        return self.handle.connection_attributes()

    def discover(self, name):
#         bname = name.encode('utf-8')
        if self.handle == None:
            raise RFCException("Invalid handle (discover)\n")
        func = self.handle.function_lookup(name)
        f = FunctionDescriptor(func)
        return f

    def close(self):
        if self.handle == None:
            raise RFCException("Invalid handle (close)\n")
        rc = self.handle.close()
        self.handle = None
        return rc


class FunctionDescriptor:
    """
    FunctionDescriptor class - must not be created by the user - automatically
    generated by a call to sapnwrfc.connection.function_lookup()
    """

    def __init__(self, handle=None):
        self.handle = handle
        self.name = self.handle.name

    def create_function_call(self):
        call = self.handle.create_function_call()
        c = FunctionCall(call)
        return c


class FunctionCall:
    """
    FunctionCall class - must not be created by the user - automatically generated by 
    a call to sapnwrfc.FunctionDescriptor.create_function_call()
    """

    def __init__(self, handle=None):
        #sys.stderr.write("inside funccall python init\n")
        self.handle = handle
        self.name = self.handle.name
        for k, v in self.handle.function_descriptor.parameters.items():
# value:    {'direction': 1, 'name': 'QUERY_TABLE', 'type': 0, 'len': 30, 'decimals': 0, 'ulen': 60}
            if v['direction'] == IMPORT:
                cpy = sapnwrfc.rfc.Import(self.function_descriptor, v['name'], v['type'], v['len'], v['ulen'], v['decimals'], None)
            elif v['direction'] == EXPORT:
                cpy = sapnwrfc.rfc.Export(self.function_descriptor, v['name'], v['type'], v['len'], v['ulen'], v['decimals'], None)
            elif v['direction'] == CHANGING:
                cpy = sapnwrfc.rfc.Changing(self.function_descriptor, v['name'], v['type'], v['len'], v['ulen'], v['decimals'], None)
            elif v['direction'] == TABLES:
                cpy = sapnwrfc.rfc.Table(self.function_descriptor, v['name'], v['type'], v['len'], v['ulen'], v['decimals'], None)
            else:
                raise RFCException("Unknown parameter type: %d\n" % v['direction'])
            self.handle.parameters[k] = cpy
            
    def __repr__(self):
        return "<FunctionCall %s instance at 0x%x>" % (self.name, id(self))

    def __getattr__(self, *args, **kwdargs):
#         bstr = str.encode(args[0])#bytes(args[0], encoding = "utf8")
#         
#         if bstr in self.handle.parameters:
#             return self.handle.parameters[bstr]
        if args[0] in self.handle.parameters:
#             print(args[0])
            return self.handle.parameters[args[0]]
        else:
            return None

    def __call__(self, *args, **kwdargs):
        # REFACTOR: This seems not to make too much sense here ;-)
        print("Hello!\n")

    def invoke(self):
        return self.handle.invoke()