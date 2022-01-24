import pandas as pd
import numpy as np
import boto3
from decimal import Decimal
from botocore.config import Config
from boto3.dynamodb.conditions import Key, Attr
from decimal import Decimal
import json

config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)

bucketName = "heron-pipeline"
samplesS3Key = "dynamoBackup/allSamples.csv"
heronSamplesTableName = "HeronPipelineStack-samplesTable79970940-7GVMNH99SR6F"

session = boto3.Session(aws_access_key_id='',aws_secret_access_key='')
s3_client = session.client('s3')
dynamodb = session.resource('dynamodb', region_name="eu-west-1", config=config)
samplesTable = dynamodb.Table(heronSamplesTableName)


s3_client.download_file(bucketName, samplesS3Key, "allSamples.csv")
samplesDf = pd.read_csv("allSamples.csv")

numBatches = len(samplesDf) / 20
sampleChunks = np.array_split(samplesDf,numBatches)
for i,items in enumerate(sampleChunks):
    with samplesTable.batch_writer() as batch:
        for index, row in items.iterrows():
            rowDict = row.to_dict()
            payload = json.loads(json.dumps(rowDict), parse_float=Decimal)
            # print(row['genotypeVariantConf'])
            # print(type(row['genotypeVariantConf']))
            # if isinstance(row['genotypeVariantConf'], float) :
            #     payload['genotypeVariantConf'] = "NA"
            
            print(payload)
            batch.put_item(Item=payload)

print(samplesDf.head())