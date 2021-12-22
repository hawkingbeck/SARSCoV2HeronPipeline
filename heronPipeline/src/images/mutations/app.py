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
from Bio import SeqIO
import logging
from datetime import datetime
import mutations
import translate_mutations

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler(sys.stdout)
ch.setLevel(logging.DEBUG)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)
# add ch to logger
logger.addHandler(ch)

config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)

# Columns to filter metadata dataframe to before merging
MODE_AA_MUT = "aa_mutations"
MODE_NUC_MUT = "nuc_mutations"
MODE_NUC_INDEL = "nuc_indels"


### ---------------------------------------------------- ###
###           Download config for this run               ###
### ---------------------------------------------------- ###
# Step 1. Get Env Vars
dateString = os.getenv('DATE_PARTITION')
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
sampleDataRoot = os.getenv('SEQ_DATA_ROOT')
iterationUUID = os.getenv('ITERATION_UUID')
heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
mutationsTableName = os.getenv("HERON_MUTATIONS_TABLE")
referenceFastaPrefix = os.getenv('REF_FASTA_KEY')
referenceGbPrefix = os.getenv('REF_GB_KEY')
refAAFastaS3 = os.getenv("REF_AA_KEY")
genesTsvS3Key = os.getenv('GENES_TSV_KEY')
geneOverlapTsvS3Key = os.getenv('GENES_OVERLAP_TSV_KEY')
threads = os.getenv('GO_FASTA_THREADS')

# Step 2. Create resources
s3 = boto3.resource('s3', region_name='eu-west-1')
bucket = s3.Bucket(bucketName)
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
sequencesTable = dynamodb.Table(heronSequencesTableName)
mutTable = dynamodb.Table(mutations)
callDate = int(datetime.now().timestamp())

# Step 3. Create local paths
metadataLocalFilename = "/tmp/metdatata.tsv"
fastaJsonLocalFilename = "/tmp/consensus.fa"
referenceFastaLocalFilename = "/tmp/ref.fa"
referenceGbLocalFilename = "/tmp/ref.gb"
refAAFastaLocalFilename = "/tmp/ref_aa.fa"
samLocalFilename = "/tmp/sample.aligned.sam"
alignedFastaLocalFilename = "/tmp/sample.aligned.fasta"
outputAAMutTsvLocalFilename = "/tmp/sample.aa_mut.fa"
outputNucMutTsvLocalFilename = "/tmp/sample.nuc_mut.fa"
genesTsvLocalFilename = "/tmp/genes.tsv"
geneOverlapTsvLocalFilename = "/tmp/gene_overlap.tsv"
outputNucIndelLocalFilenamePrefix = "/tmp/sample.nuc_indel"
outputNucDelTsvLocalFilename = outputNucIndelLocalFilenamePrefix + ".deletions.tsv"
outputNucInsTsvLocalFilename = outputNucIndelLocalFilenamePrefix + ".insertions.tsv"
outputSnpAALinkTsvLocalFilename = "/tmp/snp_aa_link.tsv"
outputDelNucAALinkTsvLocalFilename = "/tmp/del_nuc_aa_link.tsv"
outputInsNucAALinkTsvLocalFilename = "/tmp/ins_nuc_aa_link.tsv"



# Step 4. Create iteration input paths
sampleDataRootSeqBatchesDir = f"{sampleDataRoot}/{dateString}/seqBatchFiles"
seqFile = f"{sampleDataRootSeqBatchesDir}/sequences_{iterationUUID}.fasta"
seqConsensusFile = f"{sampleDataRootSeqBatchesDir}/sequences_consensus{iterationUUID}.fasta"
keyFile = f"{sampleDataRootSeqBatchesDir}/sequences_{iterationUUID}.json"
messageListS3Key = f"messageLists/{dateString}/messageList{iterationUUID}.json"
messageListLocalFilename = "/tmp/messageList.json"




bucket.download_file(messageListS3Key, messageListLocalFilename)


with open(messageListLocalFilename) as messageListFile:
  # load or die
  messageList = json.load(messageListFile)

for message in messageList:
  print(f'Message: {message["consensusFastaPath"]}')
  
  consensusFastaKey = message["consensusFastaPath"]
  consensusFastaHash = message['seqHash']
  samFileS3Key = f"samFiles/{consensusFastaHash}.fasta.sam"
  
  sequenceLocalFilename = f"/tmp/seq_{consensusFastaHash}_.json"
  
  mode = "aa_mutations"
  # Load or die
  bucket.download_file(referenceFastaPrefix, referenceFastaLocalFilename)
  bucket.download_file(referenceGbPrefix, referenceGbLocalFilename)
  bucket.download_file(refAAFastaS3, refAAFastaLocalFilename)
  bucket.download_file(consensusFastaKey, sequenceLocalFilename)
  bucket.download_file(samFileS3Key, samLocalFilename)
  bucket.download_file(genesTsvS3Key, genesTsvLocalFilename)
  bucket.download_file(geneOverlapTsvS3Key, geneOverlapTsvLocalFilename)
  

  with open(sequenceLocalFilename) as fh_fasta_json_in:
      fastaDict = json.load(fh_fasta_json_in)
  alignedFastaStr = fastaDict['aligned']

  with open(alignedFastaLocalFilename, 'w') as fh_aligned_fasta_out:
    fh_aligned_fasta_out.write(alignedFastaStr)
      
  mutations.call_aa_mutations(consensusFastaHash,
                    output_tsv=outputAAMutTsvLocalFilename,
                    sam=samLocalFilename,
                    reference_fasta=referenceFastaLocalFilename,
                    reference_genbank=referenceGbLocalFilename,
                    threads=threads)
    
  mutations.call_nuc_mutations(consensusFastaHash,
                    output_tsv=outputNucMutTsvLocalFilename,
                    reference_fasta=referenceFastaLocalFilename,
                    aligned_fasta=alignedFastaLocalFilename)

  mutations.call_nuc_indels(consensusFastaHash, 
                    sam=samLocalFilename, 
                    output_prefix=outputNucIndelLocalFilenamePrefix)


  linkMutOutDf = translate_mutations.translate_snps(
                    genes_tsv=genesTsvLocalFilename, 
                    ref_nuc_fasta_filename=referenceFastaLocalFilename,
                    ref_aa_fasta_filename=refAAFastaLocalFilename,
                    nuc_mut_tsv=outputNucMutTsvLocalFilename, 
                    aa_mut_tsv=outputAAMutTsvLocalFilename,
                    snp_aa_link_tsv=outputSnpAALinkTsvLocalFilename, 
                    gene_overlap_tsv=geneOverlapTsvLocalFilename)

  linkDelOutDf = translate_mutations.translate_deletions(                        
                    genes_tsv=genesTsvLocalFilename, 
                    ref_nuc_fasta_filename=referenceFastaLocalFilename,
                    ref_aa_fasta_filename=refAAFastaLocalFilename,
                    nuc_del_tsv=outputNucDelTsvLocalFilename, 
                    del_nuc_aa_link_tsv=outputDelNucAALinkTsvLocalFilename)

  linkInsOutDf = translate_mutations.translate_insertions(
                    genes_tsv=genesTsvLocalFilename, 
                    ref_nuc_fasta_filename=referenceFastaLocalFilename,
                    ref_aa_fasta_filename=refAAFastaLocalFilename,
                    nuc_ins_tsv=outputNucInsTsvLocalFilename, 
                    ins_nuc_aa_link_tsv=outputInsNucAALinkTsvLocalFilename)

  ##############################################
  #     Update the record in dynamoDB
  ##############################################
  logger.info(f"Updating mut seqHash {consensusFastaHash}")
  for df in [linkMutOutDf, linkDelOutDf, linkInsOutDf]:
    for i, row in df.iterrows():
      mut_id = "#".join([consensusFastaHash, str(row["genome_mutation.pos"]), row["protein_mutation.gene"], str(row["protein_mutation.pos"])]) 
      print(f"Row: {row}")
      print(f"protein_mutation.alt: {row['protein_mutation.alt']}")
      response = mutTable.update_item(
        Key={'mutationId': mut_id},
        UpdateExpression="set seqHash=:sq, callDate=:cd, genome_mutation.pos=:gmp, genome_mutation.ref=:gmr, genome_mutation.alt=:gma, protein_mutation.gene=:pmg, protein_mutation.pos=:pmp, protein_mutation.ref=:pmr, protein_mutation.alt=:pma",
        ExpressionAttributeValues={
          ':sq': consensusFastaHash,
          ':cd': callDate,
          ':gmp': str(row["genome_mutation.pos"]),
          ':gmr': str(row["genome_mutation.ref"]),
          ':gma': str(row["genome_mutation.alt"]),
          ':pmg': str(row["protein_mutation.gene"]),
          ':pmp': str(row["protein_mutation.pos"]),
          ':pmr': str(row["protein_mutation.ref"]),
          ':pma': str(row["protein_mutation.alt"])
        })
      logger.debug(f"mut put response: {response}")

  response = sequencesTable.query(
      KeyConditionExpression=Key('seqHash').eq(consensusFastaHash)
    )
  logger.info(f"query response: {response}")

  logger.info(f"Updating status {consensusFastaHash}")
  ret = sequencesTable.update_item(
      Key={'seqHash': consensusFastaHash},
      UpdateExpression="set mutationCallDate=:d",
      ExpressionAttributeValues={
        ':d': callDate
      }
    )