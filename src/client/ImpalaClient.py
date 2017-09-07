from impala.dbapi import connect


class ImpalaClient:
    def __init__(self, host, port, database):
        self.host = host
        self.port = port
        self.database = database
        self.impala_conn = connect(host=host, port=port, database=database)

    def query(self, sql):
        impala_cur = self.impala_conn.cursor()
        impala_cur.execute(sql)
        return impala_cur.fetchall()

    def close(self):
        self.impala_conn.close()
