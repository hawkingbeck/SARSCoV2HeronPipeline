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


##############################################
# Step 2. Create resources
##############################################
# Create the AWS resources: S3Bucket, dynamoDB Table, etc...
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

print(f"Processing seqBatchFile: {seqConsensusFile}")

if os.path.isfile(seqFile) == True:
  command = ["pangolin", "--analysis-mode", "accurate", seqFile, "--outfile", "/tmp/outputUsher.csv"]
  print(f"Running Command: {command}")
  try:
    subprocess.run(command, check=True)
  except subprocess.CalledProcessError as e:
    print(f"Accurate mode error: {e}")
  print(f"Completed running in accurate mode")

  command = ["pangolin", "--analysis-mode", "fast", seqFile, "--outfile", "/tmp/outputPlearn.csv"]
  print(f"Running Command: {command}")
  try:
    subprocess.run(command, check=True)
  except subprocess.CalledProcessError as e:
    print(f"Fast mode error: {e}")
    
  print(f"Completed running in fast mode")

  # S3Key = f"pangolin/outputUsher.csv"
  # bucket.upload_file("/tmp/outputUsher.csv", S3Key)

  pLearnLineageDf = pd.read_csv("/tmp/outputPlearn.csv")
  usherLineageDf = pd.read_csv("/tmp/outputUsher.csv")

  pLearnLineageDf['taxon'] = [f">{f}" for f in pLearnLineageDf['taxon']]
  usherLineageDf['taxon'] = [f">{f}" for f in usherLineageDf['taxon']]
  keyFileDf = pd.read_json(keyFile, orient="records")

  pLearnJoinedDf = pd.merge(pLearnLineageDf, keyFileDf, left_on="taxon", right_on="seqId", how="inner")
  usherJoinedDf = pd.merge(usherLineageDf, keyFileDf, left_on="taxon", right_on="seqId", how="inner")

  callDate = int(datetime.now().timestamp())
  updateCount = 0

  # +++++++++++++++++++++++++++++++++++++++++
  # Update pLearn calls
  # +++++++++++++++++++++++++++++++++++++++++
  print(f"fast mode header: {pLearnJoinedDf.columns}")
  print(f"accurate mode heaer: {pLearnJoinedDf.columns}")
  for index, row in pLearnJoinedDf.iterrows():
    seqHash = row["seqHash"]
    lineage = row["lineage"]
    conflict = Decimal(str(row['conflict']))
    ambiguityScore = Decimal(str(row['ambiguity_score']))

    if np.isnan(float(conflict)):
      conflict = Decimal(0.0)
    if np.isnan(float(ambiguityScore)):
      ambiguityScore = Decimal(0.0)
    
    version = "version"
    pangolinVersion = "version"
    pangoLearnVersion = "version"
    pangoVersion = "version"
    scorpioCall = row['scorpio_call']
    
    scorpioSupport = Decimal(str(row["scorpio_support"]))
    scorpioConflict = Decimal(str(row["scorpio_conflict"]))
    scorpioNote = Decimal(str(row["scorpio_conflict"]))

    if np.isnan(float(scorpioSupport)):
      scorpioSupport = Decimal(0.0)
    if np.isnan(float(scorpioConflict)):
      scorpioConflict = Decimal(0.0)
    if not isinstance(scorpioCall, str):
      scorpioCall = "N/A"
      
    seqId = row['seqId']
    sequencesTable = dynamodb.Table(heronSequencesTableName)
    response = sequencesTable.query(KeyConditionExpression=Key('seqHash').eq(seqHash))
    if 'Items' in response:
      if len(response['Items']) == 1:
        item = response['Items'][0]
        ret = sequencesTable.update_item(
            Key={'seqHash': seqHash},
            UpdateExpression="set pangoLineage=:l, pangoCallDate=:d, pangoConflict=:c, pangoAmbiguityScore=:a, version=:v, pangolinVersion=:plnv, pangoLearnVersion=:plv, pangoVersion=:pv, scorpioCall=:sc, scorpioSupport=:ss, scorpioConflict=:sn",
            ExpressionAttributeValues={
              ':l': lineage,
              ':d': callDate,
              ':a': ambiguityScore,
              ':v': version,
              ':plnv': pangolinVersion,
              ':plv': pangoLearnVersion,
              ':pv': pangoVersion,
              ':c': conflict,
              ':sc': scorpioCall,
              ':ss': scorpioSupport,
              ':sn': scorpioConflict
            }
          )
        updateCount += 1

  print(f"Updated {updateCount} out of {len(pLearnJoinedDf)}")

  print(f"keyFileDf length: {len(keyFileDf)}")
  print(f"lineageDf length: {len(pLearnLineageDf)}")
  print(f"JoinedDf length: {len(pLearnJoinedDf)}")

  # +++++++++++++++++++++++++++++++++++++++++
  # Update Usher calls
  # +++++++++++++++++++++++++++++++++++++++++
  for index, row in usherJoinedDf.iterrows():
    seqHash = row["seqHash"]
    lineage = row["lineage"]
    seqId = row['seqId']
    pangoNote = str(row['note'])
    scorpioNote = str(row['scorpio_notes'])
    # Create query for dynamoDB
    
    sequencesTable = dynamodb.Table(heronSequencesTableName)
    response = sequencesTable.query(KeyConditionExpression=Key('seqHash').eq(seqHash))
    if 'Items' in response:
      if len(response['Items']) == 1:
        item = response['Items'][0]
        ret = sequencesTable.update_item(
            Key={'seqHash': seqHash},
            UpdateExpression="set pangoUsherLineage=:l, pangoUsherCallDate=:d, pangoNote=:n, scorpioNote=:s",
            ExpressionAttributeValues={
              ':l': lineage,
              ':d': callDate,
              ':n': pangoNote,
              ':s': scorpioNote
            }
          )
        updateCount += 1

  # print(f"Updated {updateCount} out of {len(usherJoinedDf)}")

  print(f"keyFileDf length: {len(keyFileDf)}")
  print(f"lineageDf length: {len(usherLineageDf)}")
  print(f"JoinedDf length: {len(usherJoinedDf)}")