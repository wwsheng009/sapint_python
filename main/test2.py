'''
Created on 2014-4-17

@author: wangweisheng
'''
import sapnwrfc
sapnwrfc.base.config_location = 'c:/temp/aip.yml'
sapnwrfc.base.load_config()

conn = sapnwrfc.base.rfc_connect()
print("connection attributes: ", conn.connection_attributes())
print("discover...")
fd = conn.discover("ZVI_RFC_READ_TABLE")


print("finished discover...")
f = fd.create_function_call()
f.QUERY_TABLE("MARM")
f.ROWCOUNT(3)
#f.OPTIONS( [{ 'TEXT': "NAME LIKE 'RS%'"}] )
print("do the call...")
f.invoke()
d = f.DATA.value
fields = f.FIELDS.value
for field in fields:
    print(field[b'TYPE'])

print("DATA LENG: ", len(d), " \n")

#print("PROGS[0]: ", d[0], " \n")
print("NO. PROGS: ", len(f.DATA()), " \n")

for line in d:
    print(line[b"FELD"].decode())

conn.close()
