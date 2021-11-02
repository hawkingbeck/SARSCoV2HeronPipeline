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
import difflib
# from datafunk.sam_2_fasta import *
from Bio import SeqIO
import pysam

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
# seqConsensusFile = os.getenv('SEQ_CONSENSUS_BATCH_FILE') # Path to the EFS file that we wish to process
keyFile = os.getenv('SEQ_KEY_FILE') #Path to the file that contains the sequence hash and id
referenceFastaPrefix = os.getenv('REF_FASTA_KEY')
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
messageListS3Key = os.getenv('MESSAGE_LIST_S3_KEY')
trimStart = os.getenv('TRIM_START')
trimEnd = os.getenv('TRIM_END')
iterationUUID = os.getenv('ITERATION_UUID')
sampleDataRoot = os.getenv('SEQ_DATA_ROOT')

# batchUUID = os.path.splitext(os.path.basename(seqConsensusFile))[0].replace("sequences_", "")

sampleDataRootSeqBatchesDir = f"{sampleDataRoot}/{dateString}/seqBatchFiles"

seqConsensusFile = f"/tmp/sequences_consensus_{iterationUUID}.fasta"
efsOutputConsensusFastaFile = f"{sampleDataRootSeqBatchesDir}/sequences_consensus{iterationUUID}.fasta"
outputPlacementKeyFile = f"{sampleDataRootSeqBatchesDir}/sequences_{iterationUUID}.json"


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

with open(messageListLocalFilename) as messageListFile:
   messageList = json.load(messageListFile)

# Iterate over each item in the message list and align the sample
for message in messageList:
   print(f'Message: {message["consensusFastaPath"]}')
   # Download the consensus fasta
   consensusFastaKey = message["consensusFastaPath"]
   consensusFastaHash = message['seqHash']

   try:
      bucket.download_file(consensusFastaKey, sampleLocalFilename)
   except:
      print(f"File not found: {consensusFastaKey}")
      sampleLocalFilename = None

   with open(sampleLocalFilename, 'r') as file:
      data = file.read()

   sample = json.loads(data)

   consensusFasta = sample['consensus']
   pAlignedFasta = sample['aligned']

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

   try:
      bucket.upload_file(mappedSamFastaLocalFilename, f"samFiles/{dateString}/{os.path.basename(consensusFastaKey)}.sam")
   except:
      print("Can't upload SAM file")
   ##############################################
   # Step 2. 
   ##############################################
   samfile = pysam.AlignmentFile(mappedSamFastaLocalFilename, 'r')
   reference = SeqIO.read(referenceFastaLocalFilename, 'fasta')
   
   # Run gofasta
   # gofasta sam toMultiAlign -t ${task.cpus} \
   #    --samfile ${sam} \
   #    --reference ${reference_fasta} \
   #    --pad \
   #    -o alignment.fasta
   goFastaCommand = f"/root/go/bin/gofasta sam toMultiAlign --samfile {mappedSamFastaLocalFilename} --pad -o {alignedLocalFilename}"
   subprocess.run(
      goFastaCommand,
      check=True,
      shell=True
   )

   # Run sam_2_fasta from datafunk
  #  sam_2_fasta(samfile = samfile,
  #               reference = reference,
  #               output = alignedLocalFilename,
  #               prefix_ref = False,
  #               log_inserts = False,
  #               log_all_inserts = False,
  #               log_dels = False,
  #               log_all_dels = False,
  #               trim = True,
  #               pad = True,
  #               trimstart = int(trimStart),
  #               trimend = int(trimEnd))
    
  #  ##############################################
  #  # Step 1. Write updated result into S3
  #  ##############################################
   with open(alignedLocalFilename) as file:
      alignedFasta = file.read()
   

   print(f"Previous Aligned Fasta: {pAlignedFasta[0:50]}")
   print(f"Current Aligned Fasta: {alignedFasta[0:50]}")
   if pAlignedFasta == alignedFasta:
      print("Both aligned fasta files are the same")
   else:
      print("The aligned fasta files are different")

   output_list = [li for li in difflib.ndiff(pAlignedFasta, alignedFasta) if li[0] != ' ']
   print(f"Differences: {output_list}")

  #  sample['aligned'] = alignedFasta

  #  s3.Object(bucketName, consensusFastaKey).put(Body=json.dumps(sample))

  #  ##############################################
  #  # Step 1. Update the record in dynamoDB
  #  ##############################################
  #  dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
  #  heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
  #  sequencesTable = dynamodb.Table(heronSequencesTableName)
  #  response = sequencesTable.query(
  #        KeyConditionExpression=Key('seqHash').eq(consensusFastaHash)
  #     )

  #  if 'Items' in response:
  #     if len(response['Items']) == 1:
  #        item = response['Items'][0]
  #        item['processingState'] = 'aligned'
  #        ret = sequencesTable.update_item(
  #           Key={'seqHash': consensusFastaHash},
  #           UpdateExpression="set processingState=:s",
  #           ExpressionAttributeValues={
  #              ':s': 'aligned'
  #           }
  #        )



