import psycopg2
import os
import json

class SimpleMsSqlConnection():
    def __init__(self, Database = ""):
        self.user = os.getenv('AWS_COLLECTION_DB_USER')
        self.sslmode = "require"
        self.password = os.getenv('AWS_COLLECTION_DB_PASSWORD')
        self.host = os.getenv('AWS_COLLECTION_DB_SERVER')
        self.ConnectionString = "host={0} user={1} dbname={2} password={3} sslmode={4}".format(self.host, self.user,
        Database, self.password, self.sslmode)
        

    def connect(self):
        try:
            print("Attempting to connect")
            #print(str(self.ConnectionString))
            self.connection = psycopg2.connect(self.ConnectionString, connect_timeout=30)
            print ("Connection Established")
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(e)
        #self.cursor.fast_executemany = True

    def runSQL(self, command = "", parameters = ""):
        try:
            self.connect()
            self.cursor.execute(command,parameters)
            self.connection.commit()
            data = self.cursor.fetchall()
            return data
            #for row in data:
                #return(row)

        except Exception as e:
            print(str(e))
        finally:
            self.disconnect()

    def disconnect(self):
        self.cursor.close()  # Closing connections
        self.connection.close()

def querydatabase():
    query = ""
    print("Connecting")
    try:
        connection = SimpleMsSqlConnection(Database=os.getenv('AWS_COLLECTION_DB_NAME'))
    except Exception as e:
        print(e)
    print("Querying")
    return connection.runSQL(query)
        
def main():
    print("lambda_function called")
    # output = querydatabase()
    output = json.dumps(querydatabase(), indent=4, default=str)
    # print(output)
    return output
    
main()
