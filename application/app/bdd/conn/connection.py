from bdd.config import param
import psycopg2
import pandas as pd
from datetime import datetime

class Conn:
    
    """
    connect to database
    """

    
    def __init__(self) -> None:
    
        self.__host = param.HOST 
        self.__user = param.USER
        self.__password = param.PASSWORD
        self.__database = param.DATABASE
        self.__cur = None
        self.__conn = None


    def __open(self) -> None:
        self.__conn = psycopg2.connect(
            host=self.__host,
            database=self.__database,
            user=self.__user,
            password=self.__password)
        self.__cur = self.__conn.cursor()


    def __close(self) -> None:
        self.__cur.close()
        self.__conn.close()


    def sql_list(self, sql) -> None:
        self.__open()
        for i in sql:
           self.__cur.execute(i)
        self.__conn.commit()
        self.__close()
          
          
    def __find_id_annee_week(self) -> int:
        tup = datetime.today().isocalendar()
        annee = tup[0]
        semaine = tup[1] + 1
        self.__open()
        sql = """select id from cf_flux5_week() where an={} and sem={};""".format(annee, semaine)
        self.__cur.execute(sql)
        idx = self.__cur.fetchone()[0]
        self.__close()
        return idx


    def flux_neural_network(self) -> pd.DataFrame:
        # by day
        self.__open()
        sql = """select * from cf_flux5_day() where jour<=(select current_date +10) and jour>='2016-02-11' order by jour asc;"""
        df = pd.read_sql(sql, self.__conn)
        self.__close()
        return df


    def flux_neural_network2(self) -> pd.DataFrame:
        # agg week
        idx = self.__find_id_annee_week()
        self.__open()
        sql = """select * from cf_flux5_week() where id<={};""".format(idx)
        df = pd.read_sql(sql, self.__conn)
        self.__close()
        return df


    def updates_meteo(self) -> None:
        self.__open()
        self.__cur.callproc('meteo_valeurs_manquantes')
        self.__close()




    