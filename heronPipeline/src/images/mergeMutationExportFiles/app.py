import os
import json
import gzip
import shutil
import pandas as pd
from sys import exit, stderr
from datetime import datetime
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from boto3.dynamodb.conditions import Key
import platform
from collections import defaultdict
import csv


def get_mutation(row):
  if row["proteinMutationRef"] != row["proteinMutationAlt"]:
    return f"{row['proteinMutationGene']}:{row['proteinMutationRef']}{int(row['proteinMutationPos'])}{row['proteinMutationAlt']}"
  else:
    return f"synSNP:{row['genomeMutationRef']}{int(row['genomeMutationPos'])}{row['genomeMutationAlt']}"

def extractValue(dict, param, key):
  if param in dict.keys():
    paramDict = dict[param]
    if key in paramDict.keys():
      return paramDict[key]
    else:
      return "N/A"
  else:
    return "N/A"

def createDict(dynamoItem):
  try:
    dynamoItem = json.loads(dynamoItem)
  except:
    print(f"Error loading: {dynamoItem}")
    return False, 'seqHash', 'mutation'
    
  dynamoItem = dynamoItem['Item']
  
  newDict = {
        'mutationId': extractValue(dynamoItem, 'mutationId', 'S'),
        'proteinMutationAlt': extractValue(dynamoItem, 'proteinMutationAlt', 'S'),
        'proteinMutationGene': extractValue(dynamoItem, 'proteinMutationGene', 'S'),
        'genomeMutationRef': extractValue(dynamoItem, 'genomeMutationRef', 'S'),
        'genomeMutationPos': extractValue(dynamoItem, 'genomeMutationPos', 'N'),
        'proteinMutationRef' : extractValue(dynamoItem, 'proteinMutationRef','S'),
        'proteinMutationPos': extractValue(dynamoItem, 'proteinMutationPos', 'N'),
        'seqHash' : extractValue(dynamoItem, 'seqHash', 'S'),
        'genomeMutationAlt': extractValue(dynamoItem, 'genomeMutationAlt', 'S')
  }

  mutation = get_mutation(newDict)
  return True, newDict['seqHash'], mutation

def main():
  exportArn = os.getenv("EXPORT_ARN")
  s3Prefix = os.getenv("S3_PREFIX")
  heronBucketName = os.getenv("HERON_BUCKET")
  exportFolder = os.path.basename(exportArn)

  exportManifestS3Key = f"{s3Prefix}/AWSDynamoDB/{exportFolder}/manifest-files.json"
  exportManifestLocalPath = "/tmp/manifest.json"
  concatenatedLocalFilePath = "/tmp/concatenated.csv"
  concatenatedFileS3Key = f"{s3Prefix}/AWSDynamoDB/{exportFolder}/exported.csv"
  
  print(f"exportManifestS3Key: {exportManifestS3Key}")
  print(f"concatenatedFileS3Key: {concatenatedFileS3Key}")


  #download manifest file
  s3 = boto3.resource('s3', region_name='eu-west-1')
  bucket = s3.Bucket(heronBucketName)

  bucket.download_file(exportManifestS3Key, exportManifestLocalPath)

  with open(exportManifestLocalPath) as file:
    manifestFiles = file.readlines()

  runNumber = 0
  allMutations = defaultdict(str)
  for manifestLine in manifestFiles:
    manifestItem = json.loads(manifestLine)
    dataFileKey = manifestItem['dataFileS3Key']

    # Download and unzip file
    localDataFilePathZipped = f"/tmp/{os.path.basename(dataFileKey)}"
    localDataFilePathUnZipped, ex = os.path.splitext(localDataFilePathZipped)
    
    bucket.download_file(dataFileKey, localDataFilePathZipped)
    with gzip.open(localDataFilePathZipped, 'rb') as f_in:
      with open(localDataFilePathUnZipped, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)

    print(f"Processing {localDataFilePathUnZipped}")
    with open(localDataFilePathUnZipped) as f:
      dynamoLines = f.readlines()

    frames = [createDict(f) for f in dynamoLines]
    for frame in frames:
      if frame[0] == True:
        allMutations[frame[1]] += frame[2] + "|"
    
    runNumber += 1

  allSeq = list(allMutations.keys())
  with open(concatenatedLocalFilePath, 'w') as f:
    writer = csv.writer(f)
    writer.writerow(['seqHash', 'mutations'])
    for seq in allSeq:
      writer.writerow([seq, allMutations[seq][:-1]])
  
  bucket.upload_file(concatenatedLocalFilePath, concatenatedFileS3Key)


if __name__ == '__main__':
  
  print(f"OS: {os.name}, Platform: {platform.system()}, Release: {platform.release()}")
  main()

  print("Finished")