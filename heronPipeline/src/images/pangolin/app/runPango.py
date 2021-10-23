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

if os.path.isfile(seqConsensusFile) == True:
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
    seqHash = row["seqHash"]
    lineage = row["lineage"]
    print(f"Conflict: {row['conflict']} ambiguity: {row['ambiguity_score']}")
    conflict = Decimal(str(row['conflict']))
    ambiguityScore = Decimal(str(row['ambiguity_score']))

    if np.isnan(float(conflict)):
      conflict = Decimal(0.0)
    if np.isnan(float(ambiguityScore)):
      ambiguityScore = Decimal(0.0)
    
    # pangoVersion = f"{row['version']} - {row['pangolin_version']} - {row['pangoLEARN_version']} - {row['pango_version']}"
    version = row['version']
    pangolinVersion = row['pangolin_version']
    pangoLearnVersion = row['pangoLEARN_version']
    pangoVersion = row['pango_version']
    pangoNote = str(row['note'])
    
    print(f"Scorpio Row: {row['scorpio_call']}, {row['scorpio_support']}, {row['scorpio_conflict']}")
    scorpioCall = row['scorpio_call']
    
    scorpioSupport = Decimal(str(row["scorpio_support"]))
    scorpioConflict = Decimal(str(row["scorpio_conflict"]))

    if np.isnan(float(scorpioSupport)):
      scorpioSupport = Decimal(0.0)
    if np.isnan(float(scorpioConflict)):
      scorpioConflict = Decimal(0.0)
    if not isinstance(scorpioCall, str):
      scorpioCall = "N/A"
      
    print(f"Scorpio output {scorpioCall}, {scorpioSupport}, {scorpioConflict}")
    seqId = row['seqId']
    # Create query for dynamoDB
    # taxon,lineage,conflict,ambiguity_score,scorpio_call,scorpio_support,scorpio_conflict,version,pangolin_version,pangoLEARN_version,pango_version,status,note\n
    sequencesTable = dynamodb.Table(heronSequencesTableName)
    response = sequencesTable.query(KeyConditionExpression=Key('seqHash').eq(seqHash))
    print(f"{str(type(lineage))}, {str(type(callDate))}, {str(type(ambiguityScore))}, {str(type(version))}, {str(type(pangolinVersion))}, {str(type(pangoLearnVersion))}, {str(type(pangoVersion))}, {str(type(conflict))}, {str(type(scorpioSupport))}, {str(type(scorpioConflict))}, {str(type(scorpioCall))}, {str(type(pangoNote))}")
    if 'Items' in response:
      if len(response['Items']) == 1:
        item = response['Items'][0]
        print(f"Updating: {seqHash}")
        ret = sequencesTable.update_item(
            Key={'seqHash': seqHash},
            UpdateExpression="set pangoLineage=:l, pangoCallDate=:d, pangoConflict=:c, pangoAmbiguityScore=:a, version=:v, pangolinVersion=:plnv, pangoLearnVersion=:plv, pangoVersion=:pv, scorpioCall=:sc, scorpioSupport=:ss, scorpioConflict=:sn, pangoNote=:n",
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
              ':sn': scorpioConflict,
              ':n': pangoNote
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
            UpdateExpression="set pangoUsherLineage=:l, pangoUsherCallDate=:d",
            ExpressionAttributeValues={
              ':l': lineage,
              ':d': callDate
            }
          )
        updateCount += 1

  print(f"Updated {updateCount} out of {len(usherJoinedDf)}")

  print(f"keyFileDf length: {len(keyFileDf)}")
  print(f"lineageDf length: {len(usherLineageDf)}")
  print(f"JoinedDf length: {len(usherJoinedDf)}")