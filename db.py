from datetime import datetime
from mysql.connector import (connection, errors)

#import for pyinstaller
from mysql.connector.locales.eng import client_error
import mysql.connector.plugins.mysql_native_password


class EuroDB():

    def __init__(self, user, password, host, database) -> None:
        try:
            self.connection = connection.MySQLConnection(
                user=user, password=password, host=host, database=database,
                ssl_disabled=True)
            self.cursor = self.connection.cursor(dictionary=True)
        except errors.Error as err:
            print(f"{err.msg}")

    def get_log(self) -> dict:
        result = {}
        # q = ("SELECT * FROM eurodb.tb_avanzamento WHERE codice_operatore = 'CNC01'")
        q = ("SELECT max(id_avanzamento) as max_id, count(id_avanzamento) as rec_count, max(timestamp) as ts, numero_ordine, lotto_ordine "
             "FROM tb_avanzamento ta  WHERE codice_operatore ='CNC01' GROUP BY numero_ordine, lotto_ordine")
        self.cursor.execute(q, ())
        for rec in self.cursor.fetchall():
            ordine = f"{rec['numero_ordine']}_{rec['lotto_ordine']}"
            result[ordine] = rec
        return result


    def add_log(self, ts: datetime, numero: int, lotto: str, fine: bool = False, secondi: int = 0) -> None:
        q = ("INSERT INTO eurodb.tb_avanzamento "
            "(codice_operatore, codice_fase, inizio_fine, numero_ordine, lotto_ordine, timestamp, secondi) "
            "VALUES('CNC01', 'A1', %s, %s, %s, %s, %s)")
        self.cursor.execute(q, (fine, numero, lotto, ts, secondi))

    def update_log(self, id: int, ts: datetime, sec: int = 1) -> None:
        q = ("UPDATE eurodb.tb_avanzamento "
            "SET `timestamp`=%s, secondi=%s "
            "WHERE id_avanzamento=%s")
        self.cursor.execute(q, (ts, sec, id))
        
if __name__ == '__main__':
    db = EuroDB('mecal', 'mecal', '192.168.29.96', 'eurodb')
    r = db.get_log()
    print(r)