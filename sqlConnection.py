import mysql.connector

class sqlConnection():
    def __init__(self, host='srv988.hstgr.io', user='u205008851_smarius', password='Tm130119!', database='u205008851_watches', port=3306, autocommit=True):
        self.connection = {
            'host': host,
            'user': user,
            'password': password,
            'database': database,
            'port': port,
            'autocommit': autocommit
        }
        self.mydb = None
        self.cur = None
        
        self.openMySQL()
    
    def openMySQL(self):
        self.mydb = mysql.connector.connect(
            host=self.connection['host'],
            user=self.connection['user'],
            password=self.connection['password'],
            database=self.connection['database'],
            port=self.connection['port'],
            autocommit=self.connection['autocommit']
        )
        
        # set cursor
        self.cur = self.mydb.cursor()
    
    def closeMySQL(self):
        # close cursor
        self.cur.close()
        # disconnect from server
        self.mydb.close()
