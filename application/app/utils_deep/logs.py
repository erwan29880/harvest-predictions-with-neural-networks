from datetime import datetime
from bdd.conn.connection import Conn

def logger(txt, info, page):
    nom = int(datetime.timestamp(datetime.now()))
    ml = 'cf'
    sql = """insert into ml_logs(timest, ml,info, log, page)values({}, '{}', '{}', '{}', '{}');""".format(nom, ml,info, txt, page)
    connect = Conn()
    connect.sql_list([sql])
