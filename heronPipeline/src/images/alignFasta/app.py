import os
import subprocess
import sys
import shutil
import pandas as pd
import argparse
import numpy as np
import boto3
import json
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
seqConsensusFile = os.getenv('SEQ_CONSENSUS_BATCH_FILE') # Path to the EFS file that we wish to process
keyFile = os.getenv('SEQ_KEY_FILE') #Path to the file that contains the sequence hash and id
referenceFastaPrefix = os.getenv('REF_FASTA_KEY')
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
messageListS3Key = os.getenv('MESSAGE_LIST_S3_KEY')

batchUUID = os.path.splitext(os.path.basename(seqConsensusFile))[0].replace("sequences_", "")


print(f"Processing seqBatchFile: {seqConsensusFile}")
# Create the AWS resources: S3Bucket, dynamoDB Table, etc...
s3 = boto3.resource('s3', region_name='eu-west-1')
bucket = s3.Bucket(bucketName)
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
sequencesTable = dynamodb.Table(heronSequencesTableName)

sampleLocalFilename = "/tmp/sample.fasta"
consensusLocalFilename = "/tmp/consensus.fa"
referenceFastaLocalFilename = "/tmp/ref.fa"
mappedSamFastaLocalFilename = "/tmp/sample.mapped.sam"
alignedLocalFilename = "/tmp/aligned.fa"

messageListLocalFilename = "/tmp/messageList.json"


bucket.download_file(referenceFastaPrefix, referenceFastaLocalFilename)
bucket.download_file(messageListS3Key, messageListLocalFilename)

messageList = json.load(messageListLocalFilename)

# Iterate over each item in the message list and align the sample
for message in messageList:
   print(f"Message: {message.consensusFastaPath}")
   # Download the consensus fasta
   consensusFastaKey = message.consensusFastaPath

   try:
      bucket.download_file(consensusFastaKey, sampleLocalFilename)
   except:
      print(f"File not found: {consensusFastaKey}")

   with open(sampleLocalFilename, 'r') as file:
      data = file.read()

   sample = json.loads(data)

   consensusFasta = sample['consensus']

   with open(consensusLocalFilename, 'w') as file:
      file.write(consensusFasta)
   
   ##############################################
   # Step 1. 
   ##############################################
    # Run minimap 
      # -a:  output in sam
      # -x asm5:  asm-to-ref mapping, for ~0.1% sequence divergence
   subprocess.run(
      "./minimap2 -t minimap2_threads -a -x asm5 {} {} > {}".format(
         referenceFastaLocalFilename, consensusLocalFilename, mappedSamFastaLocalFilename
      ),
      check=True,
      shell=True
   )
# Open and iterate over the file
# seqConsensusFile
# sequences = list()
# with open(seqConsensusFile) as seqFile:
#    sequences = seqFile.readlines()

# numSequences = len(sequences) / 2

# for i in range(numSequences):
#    seqId = sequences[i*2]
#    sequenceData = sequences[(i*2)+1]





