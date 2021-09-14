import pandas as pd
import numpy as np
import os
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key, Attr
import json
import math



def lambda_handler(event, context):
    """Lambda function which finds the approx number of messages in an SQS queue

    Parameters
    ----------
    event: dict, required
        Input event to the Lambda function

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        dict: array with the desired map state
    """

    reprocessingQueueName = os.getenv("HERON_PROCESSING_QUEUE")
    dailyProcessingQueueName = os.getenv("HERON_DAILY_PROCESSING_QUEUE")
    executionMode = event['executionMode']
    
    queueName = dailyProcessingQueueName
    if executionMode == "REPROCESS":
      queueName = reprocessingQueueName


    # Create the queue object
    sqs = boto3.resource('sqs')
    queue = sqs.Queue(queueName)
    queue.load()

    attributes = queue.attributes
    mapStateSize = math.ceil(int(attributes['ApproximateNumberOfMessages'])/1000)
    mapStateSize2 = math.ceil(int(attributes['ApproximateNumberOfMessages'])/40000)
    processSequencesMapConfig = [{'id': f} for f in range(mapStateSize)]
    nestedProcessConfig = [{'id': f} for f in range(40)]
    manageProcessSequencesBatchMapConfig = [{'id': f, 'process': nestedProcessConfig} for f in range(mapStateSize2)]

    return {
      'processSequencesMapConfig': processSequencesMapConfig,
      'manageProcessSequencesBatchMapConfig': manageProcessSequencesBatchMapConfig,
      'messageCount': attributes['ApproximateNumberOfMessages'],
      'queueName': queueName
    }




    
      

    

