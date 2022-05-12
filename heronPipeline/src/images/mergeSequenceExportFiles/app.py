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
  dynamoItem = json.loads(dynamoItem)
  dynamoItem = dynamoItem['Item']
  
  newDict = {
        'seqHash': extractValue(dynamoItem, 'seqHash', 'S'),
        'pangoAmbiguityScore': extractValue(dynamoItem, 'pangoAmbiguityScore', 'N'),
        'armadillinLineage': extractValue(dynamoItem, 'armadillinLineage', 'S'),
        'scorpioCall': extractValue(dynamoItem, 'scorpioCall', 'S'),
        'scorpioSupport': extractValue(dynamoItem, 'scorpioSupport', 'N'),
        'pangoNote' : extractValue(dynamoItem, 'pangoNote','S'),
        'pangoUsherLineage': extractValue(dynamoItem, 'pangoUsherLineage', 'S'),
        'pangoConflict' : extractValue(dynamoItem, 'pangoConflict', 'N'),
        'pangoLearnVersion': extractValue(dynamoItem, 'pangoLearnVersion', 'S'),
        'genotypeVariantConf' : extractValue(dynamoItem, 'genotypeVariantConf', 'S'),
        'pangoVersion': extractValue(dynamoItem, 'pangoVersion', 'S'),
        'scorpioConflict' : extractValue(dynamoItem, 'scorpioConflict', 'N'),
        'version': extractValue(dynamoItem, 'version', 'S'),
        'pangoLineage': extractValue(dynamoItem, 'pangoLineage', 'S'),
        'consensusFastaPath': extractValue(dynamoItem, 'consensusFastaPath', 'S'),
        'genotypeVariant': extractValue(dynamoItem, 'genotypeVariant', 'S'),
        'pctCoveredBases' : extractValue(dynamoItem, 'pctCoveredBases', 'N'),
        'pangolinVersion': extractValue(dynamoItem, 'pangolinVersion', 'S'),
        'numAlignedReads' : extractValue(dynamoItem, 'numAlignedReads', 'N'),
        'genotypeProfile' : extractValue(dynamoItem, 'genotypeProfile', 'S'),
        'genotypeCallDate': extractValue(dynamoItem, 'genotypeCallDate', 'N'),
        'pangoCallDate': extractValue(dynamoItem, 'pangoCallDate', 'N'),
  }

  return newDict

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

  allDicts = []
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
      dynamoLines = f.readlines()

    frames = [createDict(f) for f in dynamoLines]
    allDicts.extend(frames)
    
  # Save the resulting dataframe back into S3
  exportDf = pd.DataFrame(allDicts)
  exportDf.to_csv(concatenatedLocalFilePath, index=False)
  bucket.upload_file(concatenatedLocalFilePath, concatenatedFileS3Key)


if __name__ == '__main__':
  main()

  print("Finished")