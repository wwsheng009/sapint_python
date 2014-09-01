

#python mysql test

import mysql.connector
from mysql.connector import errorcode

config = {
  'user': 'root',
  'password': 'root',
  'host': '127.0.0.1',
  'database': 'sapdb',
  'raise_on_warnings': True,
}

try:
  cnx = mysql.connector.connect(**config)
except mysql.connector.Error as err:
  if err.errno == errorcode.ER_ACCESS_DENIED_ERROR:
    print("Something is wrong with your user name or password")
  elif err.errno == errorcode.ER_BAD_DB_ERROR:
    print("Database does not exists")
  else:
    print(err)
    cnx.close()
    
    
cursor = cnx.cursor()

cursor.execute("SHOW TABLES LIKE 'VBAK'")
print(cursor.fetchone())
#print(cursor.fetchmany())
#print(cursor.fetchall())
print(cursor.rowcount)
