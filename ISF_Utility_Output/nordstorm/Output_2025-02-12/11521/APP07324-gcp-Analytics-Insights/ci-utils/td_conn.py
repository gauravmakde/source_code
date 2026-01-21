import teradatasql  
import logging
  
class TeradataConnection:  
    def __init__(self, host, username, password, port):  
        self.host = host  
        self.username = username  
        self.password = password  
        self.port = port
        self.connection = None  
      
    def connect(self):  
        """  
        Establishes a connection to the Teradata database.  
        """  
        try:  
            self.connection = teradatasql.connect(  
                host=self.host,  
                user=self.username,  
                password=self.password,
                dbs_port=self.port
            )
            self.connection.autocommit = True  
            logging.info("TD Connection established successfully. Autocommit is ON")  
        except Exception as e:  
            logging.error("Error connecting to Teradata database:", e)  
            raise  
      
    def disconnect(self):  
        """  
        Closes the connection to the Teradata database.  
        """  
        if self.connection:  
            self.connection.close()  
            self.connection = None  
            logging.info("TD connection closed.")  
      
    def drop_query(self, table):  
        """  
        Executes a DROP SQL query on the Teradata database.  
        """  
        if self.connection is None:  
            print("Not connected to the database.")  
            return None  
        query = (f"DROP TABLE {table};")
        try:  
            with self.connection.cursor() as cursor:  
                cursor.execute(query)
            logging.info(f"Successfully dropped {table}")
        except Exception as e:  
            logging.error(f"Error while dropping table {table}")
            raise e