

from sqlalchemy import create_engine
from sqlalchemy import Table,Column,Integer,String,MetaData,ForeignKey


#mysql+mysqlconnector://<user>:<password>@<host>[:<port>]/<dbname>
engine = create_engine('mysql+mysqlconnector://root:root@localhost/sapdb')

metadata = MetaData()

users = Table('users')
metadata