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

config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)


def main():
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Read environment variables
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  heronMutationsTableName = os.getenv("HERON_MUTATIONS_TABLE")
  heronBucketName = os.getenv("HERON_SAMPLES_BUCKET")
  dateString = os.getenv("DATE_PARTITION")
  executionId = os.getenv("EXECUTION_ID")

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Create AWS resource clients
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  sqs = boto3.resource('sqs')
  dynamodbClient = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
  mutationsTable = dynamodbClient.Table(heronMutationsTableName)


  s3 = boto3.resource('s3', region_name='eu-west-1')
  bucket = s3.Bucket(heronBucketName)

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Extract all data from the mutations table
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  scan_kwargs = dict()
  startKey = "N/A"
  sequencesDf = pd.DataFrame()
  while startKey is not None:
    response = mutationsTable.scan(**scan_kwargs)
    if len(response['Items']) > 0:
      startKey = response.get('LastEvaluatedKey', None)
      scan_kwargs['ExclusiveStartKey'] = startKey
      sequencesDf = sequencesDf.append(pd.DataFrame(response['Items']))
      
  print(f"Extracted {len(sequencesDf)} mutations")
  sequencesDf.to_csv("/tmp/mutations.csv", index=False)
  bucket.upload_file("/tmp/mutations.csv", f"results/{dateString}/allMutations.csv")

