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
  command = ["pangolin", "--analysis-mode", "accurate", seqFile, "--outfile", "/tmp/outputAccurate.csv"]
  print(f"Running Command: {command}")
  try:
    subprocess.run(command, check=True)
  except subprocess.CalledProcessError as e:
    print(f"Accurate mode error: {e}")
  print(f"Completed running in accurate mode")

  # command = ["pangolin", "--analysis-mode", "fast", seqFile, "--outfile", "/tmp/outputFast.csv"]
  # print(f"Running Command: {command}")
  # try:
  #   subprocess.run(command, check=True)
  # except subprocess.CalledProcessError as e:
  #   print(f"Fast mode error: {e}")

  # pLearnLineageDf = pd.read_csv("/tmp/outputFast.csv")
  # usherLineageDf = pd.read_csv("/tmp/outputAccurate.csv")  
  # fastModeDf = pd.read_csv("/tmp/outputFast.csv")
  accurateModeDf = pd.read_csv("/tmp/outputAccurate.csv")

  # fastModeDf['taxon'] = [f">{f}" for f in fastModeDf['taxon']]
  accurateModeDf['taxon'] = [f">{f}" for f in accurateModeDf['taxon']]
  keyFileDf = pd.read_json(keyFile, orient="records")

  # fastModeJoinedDf = pd.merge(fastModeDf, keyFileDf, left_on="taxon", right_on="seqId", how="inner")
  accurateModeJoinedDf = pd.merge(accurateModeDf, keyFileDf, left_on="taxon", right_on="seqId", how="inner")

  callDate = int(datetime.now().timestamp())
  updateCount = 0

  # +++++++++++++++++++++++++++++++++++++++++
  # Update accurate mode pangolin calls
  # +++++++++++++++++++++++++++++++++++++++++
  for index, row in accurateModeJoinedDf.iterrows():
    seqHash = row["seqHash"]
    lineage = row["lineage"]
    conflict = Decimal(str(row['conflict']))
    ambiguityScore = Decimal(str(row['ambiguity_score']))
    scorpioCall = row['scorpio_call']
    scorpioSupport = Decimal(str(row["scorpio_support"]))
    scorpioConflict = Decimal(str(row["scorpio_conflict"]))
    scorpioNote = str(row["scorpio_notes"])
    version = str(row["version"]) # A version number that represents both pangolin-data version number
    pangolinVersion = str(row["pangolin_version"]) # The version of pangolin software running.
    scorpioVersion = str(row["scorpio_version"]) # The version of the scorpio software installed.
    constellationVersion = str(row["constellation_version"]) # The version of constellations that scorpio has used to curate the lineage 
    isDesignated = str(row["is_designated"])
    qcStatus = str(row["qc_status"])
    qcNotes = str(row["qc_notes"])
    note = str(row["note"])
    
    
    # Clean up the results
    if np.isnan(float(conflict)):
      conflict = Decimal(0.0)
    if np.isnan(float(ambiguityScore)):
      ambiguityScore = Decimal(0.0)
    if np.isnan(float(scorpioSupport)):
      scorpioSupport = Decimal(0.0)
    if np.isnan(float(scorpioConflict)):
      scorpioConflict = Decimal(0.0)
    if not isinstance(scorpioCall, str):
      scorpioCall = "N/A"
    

    updateExpression = "remove armadillinCallDate, armadillinLineage, pangoLearnVersion, pangoUsherCallDate, pangoUsherLineage, pangoVersion, version set pangoCallDate=:pangoCallDate, pangoLineage=:pangoLineage, pangoConflict=:pangoConflict, pangoAmbiguityScore=:pangoAmbiguityScore, scorpioCall=:scorpioCall, scorpioSupport=:scorpioSupport, scorpioConflict=:scorpioConflict, scorpioNote=:scorpioNote, pangoSoftwareVersion=:pangoSoftwareVersion, pangolinVersion=:pangolinVersion, scorpioVersion=:scorpioVersion, constellationVersion=:constellationVersion, isDesignated=:isDesignated, pangoQcStatus=:qcStatus, pangoQcNotes=:qcNotes, pangoNote=:pangoNote" 
    
    seqKey = {'seqHash': seqHash}

    pangolinPayload = {
              ':pangoCallDate': callDate,
              ':pangoLineage': lineage,
              ':pangoConflict': conflict,
              ':pangoAmbiguityScore': ambiguityScore,
              ':scorpioCall': scorpioCall,
              ':scorpioSupport': scorpioSupport,
              ':scorpioConflict': scorpioConflict,
              ':scorpioNote': scorpioNote,
              ':pangoSoftwareVersion': version,
              ':pangolinVersion': pangolinVersion,
              ':scorpioVersion': scorpioVersion,
              ':constellationVersion': constellationVersion,
              ':isDesignated': isDesignated,
              ':qcStatus': qcStatus,
              ':qcNotes': qcNotes,
              ':pangoNote': note
            }


    seqId = row['seqId']
    sequencesTable = dynamodb.Table(heronSequencesTableName)
    ret = sequencesTable.update_item(
            Key=seqKey,
            UpdateExpression=updateExpression,
            ExpressionAttributeValues=pangolinPayload
          )
    updateCount += 1


    # response = sequencesTable.query(KeyConditionExpression=Key('seqHash').eq(seqHash))
    # if 'Items' in response:
    #   if len(response['Items']) == 1:
    #     item = response['Items'][0]
    #     ret = sequencesTable.update_item(
    #         Key=seqKey,
    #         UpdateExpression=updateExpression,
    #         ExpressionAttributeValues=pangolinPayload
    #       )
    #     updateCount += 1

  print(f"Updated {updateCount} out of {len(accurateModeJoinedDf)}")