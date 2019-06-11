import mysql.connector


class CurwSimAdapter:
    def __init__(self, mysql_user, mysql_password, mysql_host, mysql_db):
        self.connection = mysql.connector.connect(
                                                    user=mysql_user,
                                                    password=mysql_password,
                                                    host=mysql_host,
                                                    database=mysql_db)
        self.cursor = self.connection.cursor()

    def close_connection(self):
        self.cursor.close()
        self.connection.close()

    def get_flo2d_tms_ids(self, model, method):
        cursor = self.cursor
        try:
            sql = 'select id from curw_sim.run where model={} and method={} '.format(model, method)
            cursor.execute(sql)
            results = cursor.fetchall()
            for row in results:
                print(row)
        except Exception as e:
            print('save_init_state|Exception:', e)

