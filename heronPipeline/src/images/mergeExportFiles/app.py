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


def extractValue(dict, key):
  if key in dict.keys():
    return dict[key]
  else:
    return "N/A"

def createFrame(mutationItem):
  mutationItem = json.loads(mutationItem)
  mutationItem = mutationItem['Item']
  # print(f"Mutation Item: {mutationItem}")
  df = pd.DataFrame({
        'mutationId': extractValue(mutationItem['mutationId'], 'S'),
        'proteinMutationAlt': extractValue(mutationItem['proteinMutationAlt'], 'S'),
        'proteinMutationGene': extractValue(mutationItem['proteinMutationGene'], 'S'),
        'genomeMutationRef': extractValue(mutationItem['genomeMutationRef'], 'S'),
        'genomeMutationPos': extractValue(mutationItem['genomeMutationPos'], 'N'),
        'proteinMutationRef' : extractValue(mutationItem['proteinMutationRef'],'S'),
        'proteinMutationPos': extractValue(mutationItem['proteinMutationPos'], 'N'),
        'seqHash' : extractValue(mutationItem['seqHash'], 'S'),
        'genomeMutationAlt': extractValue(mutationItem['genomeMutationAlt'], 'S')
  }, index=[1])

  return df

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

  exportDf = pd.DataFrame()
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


    with open(localDataFilePathUnZipped) as f:
      mutationLines = f.readlines()

    frames = [createFrame(f) for f in mutationLines]
    if len(exportDf) == 0:
      exportDf = pd.concat(frames, ignore_index=True)
    else:
      frames.append(exportDf)
      exportDf = pd.concat(frames, ignore_index=True)

  # Save the resulting dataframe back into S3
  exportDf.to_csv(concatenatedLocalFilePath, index=False)
  bucket.upload_file(concatenatedLocalFilePath, concatenatedFileS3Key)


if __name__ == '__main__':
  main()

  print("Finished")