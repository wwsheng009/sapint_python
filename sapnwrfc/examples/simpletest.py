import sapnwrfc
import yaml
sapnwrfc.base.config_location = 'c:/temp/aip.yml'
sapnwrfc.base.load_config()

conn = sapnwrfc.base.rfc_connect()
  print("connection attributes: ", conn.connection_attributes())
  print("discover...")
  fd = conn.discover("RFC_READ_TABLE")