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
###         Function Defintions for mutations            ###
### ---------------------------------------------------- ###

def call_aa_mutations(seqHash, output_tsv, sam, reference_fasta, reference_genbank, threads):

    # From https://github.com/cov-ert/gofasta/blob/master/cmd/variants.go:
    # The output is a csv-format file with one line per query sequence, and two columns: 'query' and
    # 'variants', the second of which is a "|"-delimited list of amino acid changes and synonymous SNPs
    # in that query relative to the reference sequence specified using --reference/-r.
    # EG)  synSNP:C913T|synSNP:C3037T|orf1ab:T1001I|orf1ab:A1708D|synSNP:C5986T|orf1ab:I2230T
    #
    # But what we want is each mutation on a separate line.
    # For now we only output aa substitutions (ie nonsynonymous substitutions)
    cmd = ["gofasta", "sam", "variants",
            "-t", str(threads),
            "--samfile", sam,
            "--reference", reference_fasta,
            "--genbank", reference_genbank,
            "--outfile", "gofasta_sam_variants.out.csv"]

    proc = subprocess.run(cmd, check=True)

  
    raw_aa_mut_df = pd.read_csv('gofasta_sam_variants.out.csv', sep=",",
                                keep_default_na=False, na_values=[], dtype=str)

    # If there are no variants, gofasta v0.03 will still output a line with query and empty variants field.
    # EG)
    # query,variants
    # Consensus_39402_2#89.primertrimmed.consensus_threshold_0.75_quality_20,
    #
    # We only want to output a row if the sample actually has amino acid variants
    raw_aa_mut_df = raw_aa_mut_df.dropna()

    if raw_aa_mut_df.shape[0] > 0:
        # aa_mut_df = pd.concat([meta_df, raw_aa_mut_df[["variants"]]], axis=1)
        aa_mut_df = raw_aa_mut_df[["variants"]].copy()
        aa_mut_df['seqHash'] = seqHash
        split_mut_ser = aa_mut_df["variants"].str.split("|", expand=True).stack()
        split_mut_ser = split_mut_ser.reset_index(drop=True, level=1)  # to line up with df's index
        split_mut_ser.name = "aa_mutation"

        aa_mut_df = aa_mut_df.join(split_mut_ser).reset_index(drop=True)
        aa_mut_df = aa_mut_df.drop(columns=["variants"]).reset_index(drop=True)

    else:
        aa_mut_df = pd.DataFrame(columns=["seqHash"] + ["aa_mutation"])

    
    aa_mut_df.to_csv(output_tsv, sep="\t", header=True, index=False)


def call_nuc_indels(seqHash, sam, output_prefix):

    # From https://github.com/cov-ert/gofasta/blob/master/cmd/indels.go,
    # gofasta sam indels outputs a TSV for insertions and TSV for deletions.
    # One line for each insertion/deletion position.
    # Insertion columns:  ref_start, insertion, samples
    # Deletions columns:  ref_start, length, samples
    # --threshold is the minimum length of indel to be included in output
    cmd = ["gofasta", "sam", "indels",
              "-s", sam,
              "--threshold", "1",
              "--insertions-out", "gofasta_sam_indels.out.insertions.tsv",
              "--deletions-out", "gofasta_sam_indels.out.deletions.tsv"]

    proc = subprocess.run(cmd, check=True)

    # meta_df = pd.read_csv(metadata_tsv, sep="\t",
    #                       keep_default_na=False, na_values=[], dtype=str)

    raw_nuc_insert_df = pd.read_csv('gofasta_sam_indels.out.insertions.tsv', sep="\t",
                                    keep_default_na=False, na_values=[], dtype=str)

    # Drop any rows with empty mutations, just in case
    raw_nuc_insert_df = raw_nuc_insert_df.dropna()

    insert_tsv = output_prefix + ".insertions.tsv"
    # https://stackoverflow.com/questions/13269890/cartesian-product-in-pandas
    # workaround for cartesian product in pandas < v1.2

    if raw_nuc_insert_df.shape[0] > 0:
        nuc_insert_df = raw_nuc_insert_df[["ref_start", "insertion"]]
        # nuc_insert_df = (meta_df.assign(key=1)
        #                         .merge(
        #                             raw_nuc_insert_df[["ref_start", "insertion"]].assign(key=1),
        #                             how="outer", on="key")
        #                         .drop("key", axis=1))
        nuc_insert_df['seqHash'] = seqHash
    else:
        nuc_insert_df = pd.DataFrame(columns=["seqHash"] + ["ref_start", "insertion"])

    nuc_insert_df.to_csv(insert_tsv, sep="\t", header=True, index=False)

    raw_nuc_del_df = pd.read_csv('gofasta_sam_indels.out.deletions.tsv', sep="\t",
                                    keep_default_na=False, na_values=[], dtype=str)

    # Drop any rows with empty deletions, just in case
    raw_nuc_del_df = raw_nuc_del_df.dropna()

    del_tsv = output_prefix + ".deletions.tsv"

    if raw_nuc_del_df.shape[0] > 0:
        nuc_del_df = raw_nuc_del_df[["ref_start", "length"]]
        # nuc_del_df = (meta_df.assign(key=1)
        #                         .merge(
        #                             raw_nuc_del_df[["ref_start", "length"]].assign(key=1),
        #                             how="outer", on="key")
        #                         .drop("key", axis=1))
        nuc_del_df['seqHash'] = seqHash
    else:
        # nuc_del_df = pd.DataFrame(columns=meta_df.columns.tolist() + ["ref_start", "length"])
        nuc_del_df = pd.DataFrame(columns=["seqHash"] + ["ref_start", "length"])

    nuc_del_df.to_csv(del_tsv, sep="\t", header=True, index=False)


def call_nuc_mutations(seqHash, reference_fasta, aligned_fasta, output_tsv):

    # https://github.com/cov-ert/gofasta/blob/master/cmd/snps.go
    # The output is a csv-format file with one line per query sequence, and two columns:
    # 'query' and 'SNPs', the second of which is a "|"-delimited list of snps in that query
    cmd = ["gofasta", "snps",
           "-r", reference_fasta,
           "-q", aligned_fasta,
           "-o", "gofasta.snps.csv"]

    proc = subprocess.run(cmd, check=True)

    raw_nuc_mut_df = pd.read_csv('gofasta.snps.csv', sep=",",
                                keep_default_na=False, na_values=[], dtype=str)

    # If there are no SNPs, gofasta v0.03 will still output a line with query and empty SNPs field.
    # EG)
    # query,SNPs
    # Consensus_39402_2#89.primertrimmed.consensus_threshold_0.75_quality_20,
    #
    # We only want to output a row if the sample actually has SNPs
    raw_nuc_mut_df = raw_nuc_mut_df.dropna()

    # https://stackoverflow.com/questions/13269890/cartesian-product-in-pandas
    # workaround for cartesian product in pandas < v1.2
    if raw_nuc_mut_df.shape[0] > 0:
        nuc_mut_df = raw_nuc_mut_df[["SNPs"]]
        nuc_mut_df['seqHash'] = seqHash
        # nuc_mut_df = (meta_df.assign(key=1)
        #                      .merge(
        #                         raw_nuc_mut_df[["SNPs"]].assign(key=1),
        #                         how="outer", on="key")
        #                      .drop("key", axis=1))
        
        split_mut_ser = nuc_mut_df["SNPs"].str.split("|", expand=True).stack()
        split_mut_ser = split_mut_ser.reset_index(drop=True, level=1)  # to line up with df's index
        split_mut_ser.name = "SNP"

        nuc_mut_df = nuc_mut_df.join(split_mut_ser)

        nuc_mut_df = nuc_mut_df.drop(columns=["SNPs"]).reset_index(drop=True)

    else:
        nuc_mut_df = pd.DataFrame(columns=["seqHash"] + ["SNP"])

        
    nuc_mut_df.to_csv(output_tsv, sep="\t", header=True, index=False)

### ---------------------------------------------------- ###
###           Download config for this run               ###
### ---------------------------------------------------- ###
# Step 1. Get Env Vars
dateString = os.getenv('DATE_PARTITION')
bucketName = os.getenv('HERON_SAMPLES_BUCKET')
sampleDataRoot = os.getenv('SEQ_DATA_ROOT')
iterationUUID = os.getenv('ITERATION_UUID')
heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
referenceFastaPrefix = os.getenv('REF_FASTA_KEY')
referenceGbPrefix = os.getenv('REF_GB_KEY')
genesTsvS3Key = os.getenv('GENES_TSV_KEY')
geneOverlapTsvS3Key = os.getenv('GENES_OVERLAP_TSV_KEY')
threads = 2 #os.getenv('THREADS')

# Step 2. Create resources
s3 = boto3.resource('s3', region_name='eu-west-1')
bucket = s3.Bucket(bucketName)
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
sequencesTable = dynamodb.Table(heronSequencesTableName)
callDate = int(datetime.now().timestamp())

# Step 3. Create local paths
metadataLocalFilename = "/tmp/metdatata.tsv"
fastaJsonLocalFilename = "/tmp/consensus.fa"
referenceFastaLocalFilename = "/tmp/ref.fa"
referenceGbLocalFilename = "/tmp/ref.gb"
samLocalFilename = "/tmp/sample.aligned.sam"
alignedFastaLocalFilename = "/tmp/sample.aligned.fasta"
outputAAMutTsvLocalFilename = "/tmp/sample.aa_mut.fa"
outputNucMutTsvLocalFilename = "/tmp/sample.nuc_mut.fa"
genesTsvLocalFilename = "/tmp/genes.tsv"
geneOverlapTsvLocalFilename = "/tmp/gene_overlap.tsv"

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
  bucket.download_file(consensusFastaKey, sequenceLocalFilename)
  bucket.download_file(samFileS3Key, samLocalFilename)
  bucket.download_file(genesTsvS3Key, genesTsvLocalFilename)
  bucket.download_file(geneOverlapTsvS3Key, geneOverlapTsvLocalFilename)
  

  with open(sequenceLocalFilename) as fh_fasta_json_in:
      fastaDict = json.load(fh_fasta_json_in)
  alignedFastaStr = fastaDict['aligned']

  with open(alignedFastaLocalFilename, 'w') as fh_aligned_fasta_out:
    fh_aligned_fasta_out.write(alignedFastaStr)
      
  call_aa_mutations(consensusFastaHash,
                    output_tsv=outputAAMutTsvLocalFilename,
                    sam=samLocalFilename,
                    reference_fasta=referenceFastaLocalFilename,
                    reference_genbank=referenceGbLocalFilename,
                    threads=threads)
    
  call_nuc_mutations(consensusFastaHash,
                    output_tsv=outputNucMutTsvLocalFilename,
                    reference_fasta=referenceFastaLocalFilename,
                    aligned_fasta=alignedFastaLocalFilename)

  ##############################################
  # Step 1. Create resources
  ##############################################
    # metadataTsvS3 = event['message']['metadataTsv']
    # fastaJSONS3 = event['message']['consensusFastaPath']
    # samS3 = event['message']['samPath']
    # mode = event['message']['mode']
    # outputAAMutTsvS3 = event['message']['outputAAMutTsv']
    # outputNucMutTsvS3 = event['message']['outputNucMutTsv']

    
    ##############################################
    # Write updated result into S3
    ##############################################

    # bucket.upload_file(outputAAMutTsvLocalFilename, outputAAMutTsvS3)
    # bucket.upload_file(outputNucMutTsvLocalFilename, outputNucMutTsvS3)

    ##############################################
    # Step 1. Update the record in dynamoDB
    ##############################################

    # dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
    # heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
    # sequencesTable = dynamodb.Table(heronSequencesTableName)
    # response = sequencesTable.query(
    #       KeyConditionExpression=Key('seqHash').eq(seqHash)
    #     )
    # logger.info(f"response: {response}")

    # callDate = int(datetime.now().timestamp())
    # logger.info(f"Updating seqHash {seqHash}")
    # ret = sequencesTable.update_item(
    #     Key={'seqHash': seqHash},
    #     UpdateExpression="set mutationCallDate=:d",
    #     ExpressionAttributeValues={
    #       ':d': callDate
    #     }
    #   )
