import os
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

def lambda_handler(event, context):

  ##############################################
  # Get the list of messages from the input event
  ##############################################
  dateString = event['date']
  messageList = event['sampleBatch']['messageList']
  print(f"Message count: {len(messageList)}")

  ##############################################
  # Step 1. Create resources
  ##############################################
  
  bucketName = os.getenv('HERON_SAMPLES_BUCKET')
  s3 = boto3.resource('s3', region_name='eu-west-1')
  bucket = s3.Bucket(bucketName)

  ##############################################
  # Step 2. Download the messages and concat into
  #         a single file and save to EFS
  ##############################################
  sampleDataRoot = os.getenv('SEQ_DATA_ROOT')
  sampleDataRootSeqBatchesDir = f"{sampleDataRoot}/{dateString}/seqBatchFiles"
  outputFileUUID = str(uuid.uuid4())
  if not os.path.exists(sampleDataRootSeqBatchesDir):
    os.makedirs(sampleDataRootSeqBatchesDir)

  outputFastaFile = f"/tmp/sequences_{outputFileUUID}.fasta"
  outputFastaConsensusFile = f"/tmp/sequences_consensus_{outputFileUUID}.fasta"
  efsOutputFastaFile = f"{sampleDataRootSeqBatchesDir}/sequences_{outputFileUUID}.fasta"
  efsOutputConsensusFastaFile = f"{sampleDataRootSeqBatchesDir}/sequences_consensus{outputFileUUID}.fasta"
  outputPlacementKeyFile = f"{sampleDataRootSeqBatchesDir}/sequences_{outputFileUUID}.json"
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
    
    os.remove(localFilename)

  seqDf = pd.DataFrame(seqList)
  ##############################################
  # Step 3. Upload resultant files to S3 and EFS
  ##############################################
  S3Key = f"seqToPlace/{dateString}/sequences_{outputFileUUID}.fasta"
  bucket.upload_file(outputFastaFile, S3Key)
  copyfile(outputFastaFile, efsOutputFastaFile)

  S3Key = f"seqToPlace/{dateString}/sequences_consensus{outputFileUUID}.fasta"
  bucket.upload_file(outputFastaConsensusFile, S3Key)
  copyfile(outputFastaConsensusFile, efsOutputConsensusFastaFile)

  seqDf.to_json(outputPlacementKeyFile, orient="records")
  S3Key = f"seqToPlace/{dateString}/sequences_{outputFileUUID}.json"
  bucket.upload_file(outputPlacementKeyFile, S3Key)

  ##############################################
  # Step 4. Remove fasta file from tmp dir to
  #         free up space
  ##############################################
  os.remove(outputFastaFile)
  os.remove(outputFastaConsensusFile)

  return {'efsSeqFile': efsOutputFastaFile, 'efsSeqConsensusFile': efsOutputConsensusFastaFile, 'efsKeyFile': outputPlacementKeyFile}