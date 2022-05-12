import os
import os.path
import subprocess
import pandas as pd
import sys
import shutil
from shutil import copyfile
import uuid
import pandas as pd
from datetime import datetime
import argparse
import numpy as np
import time
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
# Get the list of messages from the input event
##############################################
dateString = os.getenv('DATE_PARTITION')
messageListKey = os.getenv('MESSAGE_LIST_S3_KEY')
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
sampleDataRoot = os.getenv('SEQ_DATA_ROOT')
iterationUUID = os.getenv('ITERATION_UUID')


print(f"message list key {messageListKey}")

##############################################
# Step 1. Create resources
##############################################
s3 = boto3.resource('s3', region_name='eu-west-1')
bucket = s3.Bucket(bucketName)


##############################################
# Step 2. Download the messages and concat into
#         a single file and save to EFS
##############################################
messageListLocalFilename = "/tmp/messageList.json"
bucket.download_file(messageListKey, messageListLocalFilename)
sampleDataRootSeqBatchesDir = f"{sampleDataRoot}/{dateString}/seqBatchFiles"

if not os.path.exists(sampleDataRootSeqBatchesDir):
  os.makedirs(sampleDataRootSeqBatchesDir)


outputFastaFile = f"/tmp/sequences_{iterationUUID}.fasta"
outputFastaConsensusFile = f"/tmp/sequences_consensus_{iterationUUID}.fasta"
efsOutputFastaFile = f"{sampleDataRootSeqBatchesDir}/sequences_{iterationUUID}.fasta"
efsOutputConsensusFastaFile = f"{sampleDataRootSeqBatchesDir}/sequences_consensus{iterationUUID}.fasta"
outputPlacementKeyFile = f"{sampleDataRootSeqBatchesDir}/sequences_{iterationUUID}.json"

with open(messageListLocalFilename) as json_file:
    messageList = json.load(json_file)

print(f"Message count: {len(messageList)}")

seqList = list()
with open(outputFastaFile, "w+") as outputFile:
  for message in messageList:
    s3Key = message['consensusFastaPath']
    seqHash = message['seqHash']
    localFilename = f"/tmp/{seqHash}.fa"
    try:
      bucket.download_file(s3Key, localFilename)

      with open(localFilename, "r") as faFile:
        seqData = json.load(faFile)
      alignedSeq = seqData["aligned"]
      alignedSeqId = alignedSeq.splitlines()[0]
      seqObject = {'seqId': alignedSeqId, 'seqHash': seqHash}
      seqList.append(seqObject)
      # outputFile.write(">")
      outputFile.writelines(alignedSeq)
    except:
      print(f"Could not download key: {s3Key}")
      # assert(False)
    
  
  if os.path.isfile(localFilename):
    os.remove(localFilename)

seqList = list()
with open(outputFastaConsensusFile, "w+") as outputFile:
  for message in messageList:
    s3Key = message['consensusFastaPath']
    seqHash = message['seqHash']
    localFilename = f"/tmp/{seqHash}.fa"
    try:
      bucket.download_file(s3Key, localFilename)
      with open(localFilename, "r") as faFile:
        seqData = json.load(faFile)
      alignedSeq = seqData["consensus"]
      alignedSeqId = alignedSeq.splitlines()[0]
      seqObject = {'seqId': alignedSeqId, 'seqHash': seqHash}
      seqList.append(seqObject)
      outputFile.writelines(alignedSeq)
    except:
      print(f"Could not download key: {s3Key}")
      # assert(False)
  
  if os.path.isfile(localFilename):
    os.remove(localFilename)

seqDf = pd.DataFrame(seqList)

##############################################
# Step 3. Upload resultant files to S3 and EFS
##############################################
if os.path.isfile(outputFastaFile):
  S3Key = f"seqToPlace/{dateString}/sequences_{iterationUUID}.fasta"
  bucket.upload_file(outputFastaFile, S3Key)
  copyfile(outputFastaFile, efsOutputFastaFile)

  S3Key = f"seqToPlace/{dateString}/sequences_consensus{iterationUUID}.fasta"
  bucket.upload_file(outputFastaConsensusFile, S3Key)
  copyfile(outputFastaConsensusFile, efsOutputConsensusFastaFile)

  seqDf.to_json(outputPlacementKeyFile, orient="records")
  S3Key = f"seqToPlace/{dateString}/sequences_{iterationUUID}.json"
  bucket.upload_file(outputPlacementKeyFile, S3Key)

  ##############################################
  # Step 4. Remove fasta file from tmp dir to
  #         free up space
  ##############################################
  os.remove(outputFastaFile)
  os.remove(outputFastaConsensusFile)