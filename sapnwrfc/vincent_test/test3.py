import sapnwrfc
sapnwrfc.base.config_location = 'd:/aip.yml'
sapnwrfc.base.load_config()

conn = sapnwrfc.base.rfc_connect()
print("connection attributes: ", conn.connection_attributes())
print("discover...")
fd = conn.discover("RFC_READ_TABLE")


print("finished discover...")
f = fd.create_function_call()
f.QUERY_TABLE("MARM")
f.ROWCOUNT(10)
#f.OPTIONS( [{ 'TEXT': "NAME LIKE 'RS%'"}] )
print("do the call...")
f.invoke()
d = f.DATA.value
#print("PROGS[0]: ", d[0], " \n")
print("NO. PROGS: ", len(f.DATA()), " \n")

for line in d:
	print(line["WA"])

conn.close()
