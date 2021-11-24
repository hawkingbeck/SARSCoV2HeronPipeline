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
# Step 1. Get Env Vars
##############################################
dateString = os.getenv('DATE_PARTITION')
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
sampleDataRoot = os.getenv('SEQ_DATA_ROOT')
iterationUUID = os.getenv('ITERATION_UUID')
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
armadillinOutputFilename = "/tmp/results.tsv"

##############################################
# Step 1. Create resources
##############################################
s3 = boto3.resource('s3', region_name='eu-west-1')
bucket = s3.Bucket(bucketName)
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
sequencesTable = dynamodb.Table(heronSequencesTableName)

##############################################
###### Create the input file paths ###########
##############################################
sampleDataRootSeqBatchesDir = f"{sampleDataRoot}/{dateString}/seqBatchFiles"
seqFile = f"{sampleDataRootSeqBatchesDir}/sequences_{iterationUUID}.fasta"
seqConsensusFile = f"{sampleDataRootSeqBatchesDir}/sequences_consensus{iterationUUID}.fasta"
keyFile = f"{sampleDataRootSeqBatchesDir}/sequences_{iterationUUID}.json"


##############################################
# Step 2. Prepare Inout data to compressed file
##############################################
keyFileDf = pd.read_json(keyFile, orient="records")
print(f"Processing seqBatchFile: {seqFile}")
if (os.path.isfile(seqFile) == True) & (os.path.isfile(armadillinOutputFilename) == True):
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

   callDate = int(datetime.now().timestamp())
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
               UpdateExpression="set armadillinLineage=:l, armadillinCallDate=:d",
               ExpressionAttributeValues={
               ':l': lineage,
               ':d': callDate
               }
            )
            updateCount += 1

   print(f"Updated {updateCount} out of {len(resultsJoinedDf)}")


# Exit