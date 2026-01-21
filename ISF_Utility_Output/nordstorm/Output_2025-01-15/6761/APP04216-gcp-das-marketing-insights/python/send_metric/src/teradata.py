import teradatasql


class Teradata:
    def __init__(self, host, user, password, default_database):
        self.host = host
        self.user = user
        self.password = password
        self.database = default_database

    def execute_query(self, sql):
        with teradatasql.connect(host=self.host, user=self.user, password=self.password,
                                 database=self.database) as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql)
                return cursor.fetchall()
