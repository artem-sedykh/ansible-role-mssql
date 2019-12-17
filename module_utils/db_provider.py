import pymssql


class ConnectionFactory(object):

    def __init__(self, server, user, password):
        """Constructor"""
        self.__server = server
        self.__user = user
        self.__password = password

    def connect(self, database="master", timeout=60):
        server = self.__server
        user = self.__user
        password = self.__password

        if not database:
            database = "master"

        # http://pymssql.org/en/stable/ref/pymssql.html
        conn = pymssql.connect(server=server, user=user, password=password, database=database, timeout=timeout,
                               appname="ansible_mssql_module")

        return conn

    def get_sql_server_version(self):
        _sql_command = "select serverproperty('productversion')"

        with self.connect() as conn:
            with conn.cursor() as cursor:
                cursor.execute(_sql_command)
                row = cursor.fetchone()
                version = row[0]
                return version.decode("utf-8")
