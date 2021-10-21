import os
import subprocess
import sys
import shutil
import pandas as pd
import argparse
import numpy as np
import boto3
from datetime import datetime
from decimal import Decimal
# from botocore.exceptions import ClientError
from botocore.config import Config
from boto3.dynamodb.conditions import Key

config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)

##############################################
# Step 1. Create resources
##############################################
dateString = os.getenv('DATE_PARTITION')
seqFile = os.getenv('SEQ_BATCH_FILE') # Path to the EFS file that we wish to process
seqConsensusFile = os.getenv('SEQ_CONSENSUS_BATCH_FILE') # Path to the EFS file that we wish to process
keyFile = os.getenv('SEQ_KEY_FILE') #Path to the file that contains the sequence hash and id
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
batchUUID = os.path.splitext(os.path.basename(seqFile))[0].replace("sequences_", "")
armadillinOutputFilename = "/tmp/results.tsv"

s3 = boto3.resource('s3', region_name='eu-west-1')
bucket = s3.Bucket(bucketName)
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
sequencesTable = dynamodb.Table(heronSequencesTableName)

##############################################
# Step 2. Prepare Inout data to compressed file
##############################################
keyFileDf = pd.read_json(keyFile, orient="records")
print(f"Processing seqBatchFile: {seqFile}")
if os.path.isfile(seqFile) == True:
   command = ["ls", "-all", "/tmp"]
   print(f"Running Command: {command}")
   subprocess.run(command)
   

   ##############################################
   # Step 4. Update the results in DynamoDB
   ##############################################
   resultsDf = pd.read_csv(armadillinOutputFilename, sep='\t')
   resultsDf.columns = ['taxon', 'lineage']
   resultsDf['taxon'] = [f">{f}" for f in resultsDf['taxon']]
   print(f"Results: {resultsDf.head(20)}, {len(resultsDf)}")
   print(f"keyFile: {keyFileDf.head(20)}, {len(keyFileDf)}")
   resultsJoinedDf = pd.merge(resultsDf, keyFileDf, left_on="taxon", right_on="seqId", how="inner")
   print(f"Joined Results: {len(resultsJoinedDf)}, {resultsJoinedDf.columns}")

   callDate = int(datetime(datetime.now().year, datetime.now().month, datetime.now().day, 0, 0, 0).timestamp())
   for index, row in resultsJoinedDf.iterrows():
      seqHash = row["seqHash"]
      lineage = row["lineage"]
      seqId = row['seqId']
      # Create query for dynamoDB
      updateCount = 0
      sequencesTable = dynamodb.Table(heronSequencesTableName)
      response = sequencesTable.query(KeyConditionExpression=Key('seqHash').eq(seqHash))
      if 'Items' in response:
         if len(response['Items']) == 1:
            item = response['Items'][0]
            print(f"Updating: {seqHash}")
            ret = sequencesTable.update_item(
               Key={'seqHash': seqHash},
               UpdateExpression="set pangoUsherLineage=:l, pangoUsherCallDate=:d",
               ExpressionAttributeValues={
               ':l': lineage,
               ':d': callDate
               }
            )
            updateCount += 1

   print(f"Updated {updateCount} out of {len(resultsJoinedDf)}")


# Exit