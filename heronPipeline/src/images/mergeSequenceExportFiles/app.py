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
  

  v = {
    "seqHash": "57c6ff91955da8a1b76a2fd3621c76ce",
    "consensusFastaPath": "fasta/57c6ff91955da8a1b76a2fd3621c76ce.fasta",
    "constellationVersion": "v0.1.10",
    "genotypeCallDate": 1657310939,
    "genotypeProfile": "BA.5",
    "genotypeVariant": "V-22APR-04",
    "genotypeVariantConf": "confirmed",
    "isDesignated": "False",
    "matchedGenotypeProfiles": {
      "dipped-bubbling": "confirmed",
      "plausible-scanner": "probable"
    },
    "mutationCallDate": 1657310929,
    "numAlignedReads": 2520448,
    "pangoAmbiguityScore": 0,
    "pangoCallDate": 1657441509,
    "pangoConflict": 0,
    "pangoLineage": "BA.5.2.1",
    "pangolinVersion": "4.1.1",
    "pangoNote": "Usher placements: BA.5.2.1(1/1)",
    "pangoQcNotes": "Ambiguous_content:0.02",
    "pangoQcStatus": "pass",
    "pangoSoftwareVersion": "PUSHER-v1.11",
    "pctCoveredBases": 99.44,
    "processingState": "aligned",
    "scorpioCall": "Omicron (BA.5-like)",
    "scorpioConflict": 0.02,
    "scorpioNote": "scorpio call: Alt alleles 60; Ref alleles 1; Amb alleles 0; Oth alleles 0",
    "scorpioSupport": 0.98,
    "scorpioVersion": "0.3.17"
  }



  newDict = {
        'seqHash': extractValue(dynamoItem, 'seqHash', 'S'),
        'pangoAmbiguityScore': extractValue(dynamoItem, 'pangoAmbiguityScore', 'N'),
        'scorpioCall': extractValue(dynamoItem, 'scorpioCall', 'S'),
        'scorpioSupport': extractValue(dynamoItem, 'scorpioSupport', 'N'),
        'pangoNote' : extractValue(dynamoItem, 'pangoNote','S'),
        'pangoLineage': extractValue(dynamoItem, 'pangoLineage', 'S'),
        'pangoConflict' : extractValue(dynamoItem, 'pangoConflict', 'N'),
        'genotypeVariantConf' : extractValue(dynamoItem, 'genotypeVariantConf', 'S'),
        'scorpioConflict' : extractValue(dynamoItem, 'scorpioConflict', 'N'),
        'consensusFastaPath': extractValue(dynamoItem, 'consensusFastaPath', 'S'),
        'genotypeVariant': extractValue(dynamoItem, 'genotypeVariant', 'S'),
        'pctCoveredBases' : extractValue(dynamoItem, 'pctCoveredBases', 'N'),
        'numAlignedReads' : extractValue(dynamoItem, 'numAlignedReads', 'N'),
        'genotypeProfile' : extractValue(dynamoItem, 'genotypeProfile', 'S'),
        'genotypeCallDate': extractValue(dynamoItem, 'genotypeCallDate', 'N'),
        'pangoCallDate': extractValue(dynamoItem, 'pangoCallDate', 'N'),
        'scorpioNote': extractValue(dynamoItem, 'scorpioNote', 'S'),
        'pangoQcNotes': extractValue(dynamoItem, 'pangoQcNotes', 'S'),
        'pangoQcStatus': extractValue(dynamoItem, 'pangoQcStatus', 'S'),
        'pangolinVersion': extractValue(dynamoItem, 'pangolinVersion', 'S'),
        'constellationVersion': extractValue(dynamoItem, 'constellationVersion', 'S'),
        'isDesignated': extractValue(dynamoItem, 'isDesignated', 'S'),
        'pangoSoftwareVersion': extractValue(dynamoItem, 'pangoSoftwareVersion', 'S'),
        'scorpioVersion': extractValue(dynamoItem, 'scorpioVersion', 'S')
  }

  matchedGenotypeProfiles = extractValue(dynamoItem, 'matchedGenotypeProfiles', 'M')
  if isinstance(matchedGenotypeProfiles, dict):
    keys = list(matchedGenotypeProfiles.keys())
    matchedGenotypes = ""
    for key in keys:
      conf = matchedGenotypeProfiles[key]['S']
      matchedGenotypes += f"{key}:{conf} "
    newDict['matchedGenotypeProfiles'] = matchedGenotypes[0:-1]
  else:
    newDict['matchedGenotypeProfiles'] = "N/A"
  
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

    frames = [createDict(f) for f in dynamoLines if f != '\n']
    allDicts.extend(frames)
    
  # Save the resulting dataframe back into S3
  exportDf = pd.DataFrame(allDicts)
  exportDf.to_csv(concatenatedLocalFilePath, index=False)
  bucket.upload_file(concatenatedLocalFilePath, concatenatedFileS3Key)


if __name__ == '__main__':
  main()

  print("Finished")