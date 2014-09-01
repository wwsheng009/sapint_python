import sapnwrfc
sapnwrfc.base.config_location = 'd:/aip.yml'
sapnwrfc.base.load_config()

conn = sapnwrfc.base.rfc_connect()
fd = conn.discover("ZVI_BIG_TABLE_SEARCH")

f = fd.create_function_call()

f.invoke()

conn.close()
