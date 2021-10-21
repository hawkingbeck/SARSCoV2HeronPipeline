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
dateString = os.getenv('DATE_PARTITION')
seqFile = os.getenv('SEQ_BATCH_FILE') # Path to the EFS file that we wish to process
seqConsensusFile = os.getenv('SEQ_CONSENSUS_BATCH_FILE') # Path to the EFS file that we wish to process
keyFile = os.getenv('SEQ_KEY_FILE') #Path to the file that contains the sequence hash and id
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
batchUUID = os.path.splitext(os.path.basename(seqFile))[0].replace("sequences_", "")
armadillinOutputFilename = "/tmp/armadillinOutput.tsv"

s3 = boto3.resource('s3', region_name='eu-west-1')
bucket = s3.Bucket(bucketName)
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
sequencesTable = dynamodb.Table(heronSequencesTableName)

##############################################
# Step 2. Prepare Inout data to compressed file
##############################################
print(f"Processing seqBatchFile: {seqFile}")
if os.path.isfile(seqFile) == True:
   command = ["gzip", "-c", seqFile, ">", "/tmp/seqFile.gz"]
   print(f"Running Command: {command}")
   subprocess.run(command, shell=True)

   command = ["ls", "-all", "/tmp"]
   print(f"Running Command: {command}")
   subprocess.run(command)
   ##############################################
   # Step 3. Run Armadillin
   ##############################################
   command = ["armadillin", "/tmp/seqFile.gz", ">", armadillinOutputFilename]
   print(f"Running Command: {command}")
   subprocess.run(command, shell=True)


   ##############################################
   # Step 4. Update the results in DynamoDB
   ##############################################
   resultsDf = pd.read_csv(armadillinOutputFilename, sep='\t')
   print(f"Results: {resultsDf.head()}")


# Exit