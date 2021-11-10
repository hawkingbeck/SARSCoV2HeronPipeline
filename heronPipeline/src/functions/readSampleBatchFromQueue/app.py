from datetime import datetime
from random import randint
from uuid import uuid4
import pandas as pd
import os
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key
import json

config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'adaptive'
   }
)

def lambda_handler(event, context):
    """Lambda function which reads from an SQS for messages to execute

    Parameters
    ----------
    event: dict, optional
        Input event to the Lambda function

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        dict: Object containing the messages to map for processing and the message count
    """

    #++++++++++++++++++++++++++++++++++++++++++++
    # Create config for this execution
    #++++++++++++++++++++++++++++++++++++++++++++
    queueName = event['queueName']
    sqs = boto3.resource('sqs', config=config)
    queue = sqs.Queue(queueName)

    bucketName = event['bucketName']
    s3 = boto3.resource('s3', region_name='eu-west-1')
    bucket = s3.Bucket(bucketName)

    dateString = event['date'] #os.getenv("DATE_PARTITION")


    #++++++++++++++++++++++++++++++++++++++++++++
    # Create config for this execution
    #++++++++++++++++++++++++++++++++++++++++++++
    sampleBatchSize = 8000
    stop = False
    messageList = list()
    messageReceiptHandles = list()
    while(stop == False):
      messages = queue.receive_messages(MaxNumberOfMessages=10, WaitTimeSeconds=2)
      messageList = messageList + [json.loads(f.body) for f in messages]
      messageReceiptHandles = messageReceiptHandles + [f.receipt_handle for f in messages]
      
      if len(messages) == 0:
        stop = True
      if len(messageList) >= sampleBatchSize:
        stop = True
      
      entries = [{
            'Id': str(ind),
            'ReceiptHandle': msg.receipt_handle
        } for ind, msg in enumerate(messages)]

      if len(entries) > 0:
        queue.delete_messages(Entries=entries)

    messageCount = len(messageList)

    iterationUUID = str(uuid4())
    messageListFileName = f"messageList{iterationUUID}.json"
    messageListS3Key = f"messageLists/{dateString}/{messageListFileName}"
    # Save messageList to s3 as a json file
    # with open(f"tmp/{messageListFileName}", "w") as write_file:
    #   json.dump(messageList, write_file)


    # Upload the file to S3
    s3.Object(bucketName, messageListS3Key).put(Body=json.dumps(messageList))


    messages = {'messageCount': messageCount, 'messageListS3Key': messageListS3Key, 'queueName': queueName, 'iterationUUID': iterationUUID}

    return messages