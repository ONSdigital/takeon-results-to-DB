This code was written as an aws lambda function, it takes a json payload from an sqs queue and writes it back to a database

The Lambda function is triggered as part of a pipeline where multiple lambdas interact. Data is read from the database and flows through the process and is placed on an SQS queue. 

The lambda that contains this code is triggered by the queue being updated, the message is retrieved in JSON format.

The JSON is then split up into its component parts to be entered into the database.