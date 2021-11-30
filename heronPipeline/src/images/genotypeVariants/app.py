from csv import reader
import os
from argparse import ArgumentParser
from yaml import full_load as load_yaml
from datetime import datetime, time
from sys import exit, stderr
import subprocess
import uuid
import json
import boto3
from botocore.exceptions import ClientError
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
genotypeRecipeS3Key = os.getenv('RECIPE_FILE_PATH')



##############################################
# Step 2. Create resources
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
messageListS3Key = f"messageLists/{dateString}/messageList{iterationUUID}.json"
messageListLocalFilename = "/tmp/messageList.json"
localRecipeFilename = f"/tmp/{str(uuid.uuid4())}.recipe"



callDate = int(datetime.now().timestamp())

# Download the message file that contains the references to all the sequences that we need to process
bucket.download_file(messageListS3Key, messageListLocalFilename)
# Download the recipe file that we need to assign variants from
bucket.download_file(genotypeRecipeS3Key, localRecipeFilename)

with open(messageListLocalFilename) as messageListFile:
   messageList = json.load(messageListFile)

for message in messageList:
  # Download the fasta file for this message
  print(f'Message: {message["consensusFastaPath"]}')
  # Download the consensus fasta
  consensusFastaKey = message["consensusFastaPath"]
  consensusFastaHash = message['seqHash']

  sequenceLocalFilename = f"/tmp/seq_{consensusFastaHash}_.json"

  try:
    bucket.download_file(consensusFastaKey, sequenceLocalFilename)
  except:
    print(f"File not found: {consensusFastaKey}")
    sampleLocalFilename = None

  alignedFasta = None
  with open(sequenceLocalFilename, "r") as fasta:
    seqData = json.load(fasta)
    alignedFasta = seqData['aligned']


  # Download the files as unique local filenames to avoid any clashes with /tmp directory
  localFastaFilename = f"/tmp/{str(uuid.uuid4())}.fasta"

  with open(localFastaFilename, "w") as fasta:
    fasta.write(alignedFasta)

  cmd = ["python", "genotype-variants.py", localFastaFilename, "phe-recipes.yml", "--verbose"]
  proc = subprocess.run(cmd, check=True, capture_output=True, text=True)
  vocProfile, vocVui, confidence, timestamp = proc.stdout.strip().split("\t")
  
  print(f"{vocProfile}, {vocVui}, {confidence}, {timestamp}")

  if str(vocProfile) == 'nan':
    vocProfile = "none"
  # Upsert the record for the sequence
  response = sequencesTable.query(
        KeyConditionExpression=Key('seqHash').eq(consensusFastaHash)
      )

  if 'Items' in response:
    if len(response['Items']) == 1:
      item = response['Items'][0]
      item['processingState'] = 'aligned'
      ret = sequencesTable.update_item(
          Key={'seqHash': consensusFastaHash},
          UpdateExpression="set genotypeVariant=:v, genotypeVariantConf=:c, genotypeCallDate=:d, genotypeProfile=:p",
          ExpressionAttributeValues={
            ':v': vocProfile,
            ':c': confidence,
            ':d': callDate,
            ':p': vocVui
          }
        )