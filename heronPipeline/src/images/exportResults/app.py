from datetime import datetime
from random import randint
from uuid import uuid4
import numpy as np
import pandas as pd
import math
import os
import uuid
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import json
import platform

config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)

def get_mutation(row):
  if row["proteinMutationRef"] != row["proteinMutationAlt"]:
    return f"{row['proteinMutationGene']}:{row['proteinMutationRef']}{int(row['proteinMutationPos'])}{row['proteinMutationAlt']}"
  else:
    return f"synSNP:{row['genomeMutationRef']}{int(row['genomeMutationPos'])}{row['genomeMutationAlt']}"

def main():
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Read environment variables
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  branchZero = os.getenv("BRANCH_ZERO")
  branchOne = os.getenv("BRANCH_ONE")
  branchTwo = os.getenv("BRANCH_TWO")
  heronBucketName = os.getenv("HERON_SAMPLES_BUCKET")
  dateString = os.getenv("DATE_PARTITION")
  executionId = os.getenv("EXECUTION_ID")
  
  print(f"Branch Zero: {branchZero}")
  print(f"Branch One: {branchOne}")
  print(f"Branch Two: {branchTwo}")

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Create AWS resource clients
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  s3 = boto3.resource('s3', region_name='eu-west-1')
  bucket = s3.Bucket(heronBucketName)

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Download the results files
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  mutationsFilename = "/tmp/mutationsExported.csv"
  samplesFilename = "/tmp/samplesExported.csv"
  sequencesFilesname = "/tmp/sequencesExported.csv"
  
  bucket.download_file(branchZero, mutationsFilename)
  bucket.download_file(branchOne, sequencesFilesname)
  bucket.download_file(branchTwo, samplesFilename)
  
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Load the files into memory
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  mutationsDf = pd.read_csv(mutationsFilename)
  samplesDf = pd.read_csv(samplesFilename)
  sequencesDf = pd.read_csv(sequencesFilesname)

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Append the mutations for each sequence
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # seqHashList = sequencesDf['seqHash'].to_list()
  # seqHashDict = dict.fromkeys(seqHashList, '')
  # mutationsDf.dropna(inplace=True)
  # mutationsList = mutationsDf.to_dict(orient='records')
  # for mutation in mutationsList:
  #   mutationValue = get_mutation(mutation)
  #   seqHash = mutation['seqHash']
  #   prevValue = seqHashDict[seqHash]
  #   if prevValue == '':
  #     seqHashDict[seqHash] =  mutationValue
  #   else:
  #     seqHashDict[seqHash] = str(seqHashDict[seqHash]) + "|" + mutationValue


  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Join on the seqHash
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # allKeys = seqHashDict.keys()
  # sortedKeys = sorted(allKeys)
  # sortedValues = [seqHashDict[f] for f in sortedKeys]
  # seqHashMutationDf = pd.DataFrame({'seqHash': sortedKeys, 'mutations': sortedValues})
  joinedSequences = pd.merge(sequencesDf, mutationsDf, left_on="seqHash", right_on="seqHash", how="inner")
  joinedDf = pd.merge(samplesDf, joinedSequences, left_on="consensusFastaHash", right_on="seqHash", how="inner")

  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  # Upload to S3
  #+++++++++++++++++++++++++++++++++++++++++++++++++++++
  fileName = f"{executionId}.csv"
  
  joinedDf.to_csv(f"/tmp/{fileName}", index=False)
  bucket.upload_file(f"/tmp/{fileName}", f"results/{dateString}/{fileName}")

if __name__ == '__main__':
  main()

  print(f"OS: {os.name}, Platform: {platform.system()}, Release: {platform.release()}")

  print("Finished")
        




