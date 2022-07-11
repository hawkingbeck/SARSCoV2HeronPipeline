from datetime import datetime
from random import randint
from uuid import uuid4
import numpy as np
import pandas as pd
import math
import os
import uuid
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import json
import time

config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)

class DecimalEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, Decimal):
      return float(obj)
    return json.JSONEncoder.default(self, obj)

def main():

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Read environment variables
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
  reprocessingQueueName = os.getenv("HERON_PROCESSING_QUEUE")
  dailyProcessingQueueName = os.getenv("HERON_DAILY_PROCESSING_QUEUE")
  executionMode = os.getenv("EXECUTION_MODE")

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Create AWS resource clients
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  sqs = boto3.resource('sqs')
  dynamodbClient = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
  sequencesTable = dynamodbClient.Table(heronSequencesTableName)

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Determine the mode for this execution
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  scan_kwargs = dict()

  # Select the queue, default to daily
  queueName = dailyProcessingQueueName
  if executionMode == "REPROCESS":
    queueName = reprocessingQueueName
  else:
    scan_kwargs['FilterExpression'] = Attr("processingState").eq("consensus") | Attr("pangoCallDate").not_exists() | Attr("genotypeCallDate").not_exists()
  
  # Create the queue object
  queue = sqs.Queue(queueName)

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Purge the queue so we start from a clean state
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  queue.purge()
  time.sleep(60)
  
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Start the scan and continue until all items have been 
  # read from the table
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # response = sequencesTable.scan(**scan_kwargs)
  # startKey = response.get('LastEvaluatedKey', None)
  # samples = response['Items']
  # scan_kwargs['ExclusiveStartKey'] = startKey
  
  batchCount = 0
  startKey = "N/A"
  messageCount = 0
  while startKey is not None:
    response = sequencesTable.scan(**scan_kwargs)
    if len(response['Items']) > 0:
      startKey = response.get('LastEvaluatedKey', None)
      scan_kwargs['ExclusiveStartKey'] = startKey
      numItems = len(response['Items'])
      numBatches = math.ceil(numItems / 10)
      itemsDf = pd.DataFrame(response['Items'])
    
      itemsDf = pd.DataFrame({
       'consensusFastaPath': itemsDf['consensusFastaPath'],
        'processingState': itemsDf['processingState'],
        'seqHash': itemsDf['seqHash']
      })
      
      ret = np.array_split(itemsDf,numBatches)
      for i,items in enumerate(ret):
        entries = list()
        for index, row in items.iterrows():
          groupId = str(uuid.uuid4()) #Each batch of 10 messages gets a new group ID, this will provide for higher numbers of concurrent consumers
          messageGroupId = groupId
          entry = {
            'Id': str(index),
            'MessageBody': json.dumps(row.to_dict(), cls=DecimalEncoder),
            'MessageGroupId': messageGroupId
          }
          entries.append(entry)
        
        ret = queue.send_messages(Entries=entries)

        successfullMessages = ret['Successful']
        messageCount += len(successfullMessages)
        if 'Failed' in ret:
          failedMessages = ret['Failed']
          failedMessageCount = len(failedMessages)
        else:
          failedMessageCount = 0

        while failedMessageCount > 0:
          # Try the failed messages again
          entries = list()
          for failedMessage in failedMessages:
            messageGroupId = f"group_{math.ceil(messageCount / 1000)-1}"
            itemsDfId = int(failedMessage['Id'])
            row = items[itemsDfId:itemsDfId+1]
            entry = {
              'Id': str(itemsDfId),
              'MessageBody': json.dumps(row.to_dict(), cls=DecimalEncoder),
              'MessageGroupId': messageGroupId
            }
            entries.append(entry)
            ret = queue.send_messages(Entries=entries)
            successfullMessages = ret['Successful']
            messageCount += len(successfullMessages)
            if 'Failed' in ret:
              failedMessages = ret['Failed']
              failedMessageCount = len(failedMessages)
              print(f"Failed Message Count {failedMessageCount}")
            else:
              failedMessageCount = 0
    else:
      startKey = response.get('LastEvaluatedKey', None)
      scan_kwargs['ExclusiveStartKey'] = startKey
      
    # if messageCount > 1000:
    #     break
    
  print(f"Message Count: {messageCount}")
  # Generate config for nested StepFunction map state
  mapStateSize = math.ceil(messageCount/1000)
  
  mapStateConfig = [{'id': f} for f in range(mapStateSize)]

  messages = {"messageCount": messageCount, "queueName": queueName, 'mapStateConfig': mapStateConfig}
  return {"messages": messages}




if __name__ == '__main__':
    
    main()

    print("Finished")
        
        