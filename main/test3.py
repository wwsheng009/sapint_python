'''
Created on 2014-4-18

@author: wangweisheng
'''
import sapint

log = sapint.getCommentLogger(__name__)

conn = sapint.GetDestination("EYANGDEV")

fd = conn.discover("ZVI_RFC_READ_TABLE")


print("finished discover...")
f = fd.create_function_call()
f.QUERY_TABLE("MARM")
f.ROWCOUNT(10)
#f.OPTIONS( [{ 'TEXT': "NAME LIKE 'RS%'"}] )
print("do the call...")
f.invoke()
d = f.DATA.value
fields = f.FIELDS.value
for field in fields:
    print(field['TYPE'])

#print("PROGS[0]: ", d[0], " \n")
print("NO. PROGS: ", len(f.DATA()), " \n")

for line in d:
    print(line["FELD"])

conn.close()
