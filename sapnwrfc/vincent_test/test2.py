import sapnwrfc
import yaml
sapnwrfc.base.config_location = 'c:/temp/aip.yml'
sapnwrfc.base.load_config()

conn = sapnwrfc.base.rfc_connect()
print("connection attributes: ", conn.connection_attributes())
print("discover...")
fd = conn.discover("ZVI_FM02")


print("finished discover...")
f = fd.create_function_call()
f.I_SIMP("b")
print("do the call...")
f.invoke()
print(f.E_SIMPLE.value)
conn.close()
