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
  heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
  heronSamplesTableName = os.getenv("HERON_SAMPLES_TABLE")
  heronBucketName = os.getenv("HERON_SAMPLES_BUCKET")
  dateString = os.getenv("DATE_PARTITION")
  executionId = os.getenv("EXECUTION_ID")
  

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Create AWS resource clients
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  sqs = boto3.resource('sqs')
  dynamodbClient = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
  sequencesTable = dynamodbClient.Table(heronSequencesTableName)
  samplesTable = dynamodbClient.Table(heronSamplesTableName)


  s3 = boto3.resource('s3', region_name='eu-west-1')
  bucket = s3.Bucket(heronBucketName)

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Extract all data from the sequences table
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  scan_kwargs = dict()
  startKey = "N/A"
  sequencesDf = pd.DataFrame()
  while startKey is not None:
    response = sequencesTable.scan(**scan_kwargs)
    if len(response['Items']) > 0:
      startKey = response.get('LastEvaluatedKey', None)
      scan_kwargs['ExclusiveStartKey'] = startKey
      sequencesDf = sequencesDf.append(pd.DataFrame(response['Items']))
      
  print(f"Extracted {len(sequencesDf)} sequences")
  sequencesDf.to_csv("/tmp/sequences.csv", index=False)
  bucket.upload_file("/tmp/sequences.csv", f"results/{dateString}/allSequences.csv")

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Extract all data from the samples table
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  scan_kwargs = dict()
  startKey = "N/A"
  samplesDf = pd.DataFrame()
  while startKey is not None:
    response = samplesTable.scan(**scan_kwargs)
    if len(response['Items']) > 0:
      startKey = response.get('LastEvaluatedKey', None)
      scan_kwargs['ExclusiveStartKey'] = startKey
      samplesDf = samplesDf.append(pd.DataFrame(response['Items']), ignore_index=True)

  print(f"Extracted {len(samplesDf)} samples")

  samplesDf.to_csv("/tmp/samples.csv", index=False)
  bucket.upload_file("/tmp/samples.csv", f"results/{dateString}/allSamples.csv")

  
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Join on the seqHash
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  joinedDf = pd.merge(samplesDf, sequencesDf, left_on="consensusFastaHash", right_on="seqHash", how="inner")
  print(f"JoinedDf has length {len(joinedDf)}")

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Remove any duplicated cogUkId's
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  deDupResults = joinedDf.sort_values(['cogUkId', 'pctCoveredBases', 'runCompleteDate']).drop_duplicates('cogUkId',keep='last')

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Upload to S3
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  fileName = f"{str(uuid.uuid4())}.csv"
  fileName = f"{executionId}.csv"
  
  deDupResults.to_csv(f"/tmp/{fileName}", index=False)
  bucket.upload_file(f"/tmp/{fileName}", f"results/{dateString}/{fileName}")

if __name__ == '__main__':
  main()

  print("Finished")
        




