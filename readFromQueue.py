import logging
import boto3
from botocore.exceptions import ClientError
import os
import json
import psycopg2

# Read from queue
def retrieve_sqs_messages(sqs_queue_url, num_msgs=1, wait_time=0, visibility_time=5):

    # Validate number of messages to retrieve
    if num_msgs < 1:
        num_msgs = 1
    elif num_msgs > 10:
        num_msgs = 10

    # Retrieve messages from an SQS queue
    sqs_client = boto3.client('sqs')
    try:
        msgs = sqs_client.receive_message(QueueUrl=sqs_queue_url,
                                          MaxNumberOfMessages=num_msgs,
                                          WaitTimeSeconds=wait_time,
                                          VisibilityTimeout=visibility_time)
    except ClientError as e:
        logging.error(e)
        return None

    # Return the list of retrieved messages
    json_data = msgs['Messages'][0]['Body']
    return json_data
    
# Database connection

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
    """Exercise retrieve_sqs_messages()"""

    # Assign this value before running the program
    sqs_queue_url = os.getenv('SQS_URL')
    num_messages = 1

    # Retrieve SQS messages
    msgs = retrieve_sqs_messages(sqs_queue_url, num_messages)
    msgDict = json.loads(msgs)
    return msgDict
    
querydatabase()
    
    
if __name__ == '__main__':
    main()
