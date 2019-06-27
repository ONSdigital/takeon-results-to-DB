import json
import readFromQueue
import os
import psycopg2
import lambda_function

def handler(event, context):
    # This line takes the message from the sqs trigger
    message = json.loads(event["Records"][0]['body'])

    x = SimpleMsSqlConnection()
    x.connect()
    x.querydatabase(message)
    
        

# Database connection

class SimpleMsSqlConnection():
    def __init__(self, Database = ""):
        self.user = os.getenv('AWS_COLLECTION_DB_USER')
        self.password = os.getenv('AWS_COLLECTION_DB_PASSWORD')
        print(self.password)
        self.host = os.getenv('AWS_COLLECTION_DB_SERVER')
        self.ConnectionString = "host={0} user={1} dbname={2} password={3}".format(self.host, self.user,
        os.getenv("AWS_COLLECTION_DB_NAME"), self.password)
        

    def connect(self):
        try:
            print("Attempting to connect")
            self.connection = psycopg2.connect(self.ConnectionString, connect_timeout=30)
            print ("Connection Established")
            self.cursor = self.connection.cursor()
        except Exception as e:
            print(e)

    def runSQL(self, command = "", parameters = ""):
        try:
            self.connect()
            self.cursor.execute(command,parameters)
            self.connection.commit()
            data = self.cursor.fetchall()
            return data

        except Exception as e:
            print(str(e))
        finally:
            self.disconnect()

    def disconnect(self):
        self.cursor.close()  # Closing connections
        self.connection.close()

    def querydatabase(self, message):
        # This line can be used instead of 'message = json.loads(event["Records"][0]['body'])' for local testing
        # message_body = <Example JSON here>
        r = message["key1"]["value1"]
        p = message["key1"]["value2"]
        s = message["key1"]["value3"]
        # Perform a delete to ensure the same data is not entered twice
        delete_query = "delete from [TABLE] where [Column1] = %s and [Column2] = %s and [Column3] = %s; commit;"
        cursor = self.connection.cursor()
        cursor.execute(delete_query, [r, p, s])
        for i in message["key2"]:
            q = i["value1"]
            ru = i["value2"]
            v = i["value3"].split("!=") #In our case this data has to be split to be inserted to two different columns
            p = v[1]
            f = v[0] + " != ''"
        # Nested select query inside values to extract information from other tables
            query = """
            insert into [TABLE] (Column1, Column2, Column3, Column4, Column5, Column6, Column7, Column8, Column9)
            values(%s, %s, %s, (select Column1 from [TABLE1] as T1, [TABLE2] as T2 where T1.Column = T2.Column and T2.Column1 = %s and T2.Column2 = %s and T2.Column3 = %s and T1.Column4 = %s and T1.Column5 = %s), '0', %s, %s, user, current_timestamp); commit;"""
            
            data = (r, p, s, r, p, s, ru, q, p, f)
            print (data)
            print("Connecting")
            print("Querying")
            cursor = self.connection.cursor()
            cursor.execute(query, data)

        
        

    