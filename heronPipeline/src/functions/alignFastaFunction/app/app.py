import os
import subprocess
import sys
import shutil
import pandas as pd
import argparse
import numpy as np
import time
import json
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from boto3.dynamodb.conditions import Key
from urllib.parse import urlparse
from datafunk.sam_2_fasta import *
from Bio import SeqIO
import pysam


config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)
def handler(event, context): 
    
    ##############################################
    # Step 1. Create resources
    ##############################################
    # Get from event input

    print(f"Event: {event}")

    consensusFastaKey = event['message']['consensusFastaPath']
    consensusFastaHash = event['message']['seqHash']
    
    
    # Get from environment variables
    bucketName = os.getenv('HERON_SAMPLES_BUCKET')
    referenceFastaPrefix = os.getenv('REF_FASTA_KEY')
    trimStart = os.getenv('TRIM_START')
    trimEnd = os.getenv('TRIM_END')

    s3 = boto3.resource('s3', region_name='eu-west-1')
    bucket = s3.Bucket(bucketName)
    sampleLocalFilename = "/tmp/sample.fasta"
    consensusLocalFilename = "/tmp/consensus.fa"
    referenceFastaLocalFilename = "/tmp/ref.fa"
    mappedSamFastaLocalFilename = "/tmp/sample.mapped.sam"
    alignedLocalFilename = "/tmp/aligned.fa"
    
    bucket.download_file(consensusFastaKey, sampleLocalFilename)
    bucket.download_file(referenceFastaPrefix, referenceFastaLocalFilename)

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

    ##############################################
    # Step 1. 
    ##############################################
    samfile = pysam.AlignmentFile(mappedSamFastaLocalFilename, 'r')
    reference = SeqIO.read(referenceFastaLocalFilename, 'fasta')
    
    # Run sam_2_fasta from datafunk
    sam_2_fasta(samfile = samfile,
                reference = reference,
                output = alignedLocalFilename,
                prefix_ref = False,
                log_inserts = False,
                log_all_inserts = False,
                log_dels = False,
                log_all_dels = False,
                trim = True,
                pad = True,
                trimstart = int(trimStart),
                trimend = int(trimEnd))
    
    ##############################################
    # Step 1. Write updated result into S3
    ##############################################
    with open(alignedLocalFilename) as file:
      alignedFasta = file.read()
    
    sample['aligned'] = alignedFasta

    s3.Object(bucketName, consensusFastaKey).put(Body=json.dumps(sample))        

    ##############################################
    # Step 1. Update the record in dynamoDB
    ##############################################
    dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
    heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
    sequencesTable = dynamodb.Table(heronSequencesTableName)
    response = sequencesTable.query(
          KeyConditionExpression=Key('seqHash').eq(consensusFastaHash)
        )

    if 'Items' in response:
      if len(response['Items']) == 1:
        item = response['Items'][0]
        item['processingState'] = 'aligned'
        ret = sequencesTable.update_item(
            Key={'seqHash': consensusFastaHash},
            UpdateExpression="set processingState=:s",
            ExpressionAttributeValues={
              ':s': 'aligned'
            }
          )