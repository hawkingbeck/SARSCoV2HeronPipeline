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

# Obtain the config for this run from the
dateString = os.getenv('DATE_PARTITION')
seqFile = os.getenv('SEQ_BATCH_FILE') # Path to the EFS file that we wish to process
seqConsensusFile = os.getenv('SEQ_CONSENSUS_BATCH_FILE') # Path to the EFS file that we wish to process
keyFile = os.getenv('SEQ_KEY_FILE') #Path to the file that contains the sequence hash and id
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
batchUUID = os.path.splitext(os.path.basename(seqFile))[0].replace("sequences_", "")

print(f"Processing seqBatchFile: {seqConsensusFile}")
# Create the AWS resources: S3Bucket, dynamoDB Table, etc...
s3 = boto3.resource('s3', region_name='eu-west-1')
bucket = s3.Bucket(bucketName)
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
sequencesTable = dynamodb.Table(heronSequencesTableName)

# Print the pango version

print(f"Pangolin D Version")
command = ["pangolin", "-dv"]
subprocess.run(command)

print(f"Pangolin Version")
command = ["pangolin", "-v"]
subprocess.run(command)

print(f"PangoLearn Version")
command = ["pangolin", "-pv"]
subprocess.run(command)

command = ["pangolin", "--verbose", "--usher", seqConsensusFile, "--outfile", "/tmp/outputUsher.csv", "--alignment"]
print(f"Running Command: {command}")
subprocess.run(command)

command = ["pangolin", "--verbose", seqConsensusFile, "--outfile", "/tmp/outputPlearn.csv", "--alignment"]
print(f"Running Command: {command}")
subprocess.run(command)

# S3Key = f"pangolin/outputUsher.csv"
# bucket.upload_file("/tmp/outputUsher.csv", S3Key)

pLearnLineageDf = pd.read_csv("/tmp/outputPlearn.csv")
usherLineageDf = pd.read_csv("/tmp/outputUsher.csv")

pLearnLineageDf['taxon'] = [f">{f}" for f in pLearnLineageDf['taxon']]
usherLineageDf['taxon'] = [f">{f}" for f in usherLineageDf['taxon']]
keyFileDf = pd.read_json(keyFile, orient="records")

pLearnJoinedDf = pd.merge(pLearnLineageDf, keyFileDf, left_on="taxon", right_on="seqId", how="inner")
usherJoinedDf = pd.merge(usherLineageDf, keyFileDf, left_on="taxon", right_on="seqId", how="inner")

callDate = int(datetime(datetime.now().year, datetime.now().month, datetime.now().day, 0, 0, 0).timestamp())
updateCount = 0

# +++++++++++++++++++++++++++++++++++++++++
# Update pLearn calls
# +++++++++++++++++++++++++++++++++++++++++
for index, row in pLearnJoinedDf.iterrows():
  # taxon,lineage,conflict,ambiguity_score,scorpio_call,scorpio_support,scorpio_conflict,version,pangolin_version,pangoLEARN_version,pango_version,status,note\n
  seqHash = row["seqHash"]
  lineage = row["lineage"]
  print(f"Conflict: {row['conflict']} ambiguity: {row['ambigutity_score']}")
  conflict = Decimal(str(row['conflict']))
  ambiguityScore = Decimal(str(row['ambiguity_score']))

  if np.isnan(float(conflict)):
    conflict = Decimal(0.0)
  if np.isnan(float(ambiguityScore)):
    ambiguityScore = Decimal(0.0)
  
  pangoVersion = f"{row['version']} - {row['pangolin_version']} - {row['pangoLEARN_version']} - {row['pango_version']}"

  seqId = row['seqId']
  # Create query for dynamoDB
  
  sequencesTable = dynamodb.Table(heronSequencesTableName)
  response = sequencesTable.query(KeyConditionExpression=Key('seqHash').eq(seqHash))
  if 'Items' in response:
    if len(response['Items']) == 1:
      item = response['Items'][0]
      print(f"Updating: {seqHash}")
      ret = sequencesTable.update_item(
          Key={'seqHash': seqHash},
          UpdateExpression="set pangoLineage=:l, pangoCallDate=:d, pangoConflict=:c, pangoCalled=:p, pangoAmbiguityScore=:a, pangoVersion=:v",
          ExpressionAttributeValues={
            ':l': lineage,
            ':d': callDate,
            ':p': 'true',
            ':a': ambiguityScore,
            ':v': pangoVersion,
            ':c': conflict
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
  # Create query for dynamoDB
  
  sequencesTable = dynamodb.Table(heronSequencesTableName)
  response = sequencesTable.query(KeyConditionExpression=Key('seqHash').eq(seqHash))
  if 'Items' in response:
    if len(response['Items']) == 1:
      item = response['Items'][0]
      print(f"Updating: {seqHash}")
      ret = sequencesTable.update_item(
          Key={'seqHash': seqHash},
          UpdateExpression="set pangoUsherLineage=:l, pangoUsherCallDate=:d, pangoUserCalled=:p",
          ExpressionAttributeValues={
            ':l': lineage,
            ':d': callDate,
            ':p': 'true'
          }
        )
      updateCount += 1

print(f"Updated {updateCount} out of {len(usherJoinedDf)}")

print(f"keyFileDf length: {len(keyFileDf)}")
print(f"lineageDf length: {len(usherLineageDf)}")
print(f"JoinedDf length: {len(usherJoinedDf)}")