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
sequencesS3Key = "dynamoBackup/allSequences.csv"
heronSequencesTableName = "HeronPipelineStack-sequencesTableAE1FA49B-JC9JE7I63NG3"

session = boto3.Session(aws_access_key_id='',aws_secret_access_key='')
s3_client = session.client('s3')
dynamodb = session.resource('dynamodb', region_name="eu-west-1", config=config)
sequencesTable = dynamodb.Table(heronSequencesTableName)


s3_client.download_file(bucketName, sequencesS3Key, "allSequences.csv")
sequencesDf = pd.read_csv("allSequences.csv")

numBatches = len(sequencesDf) / 20
sequenceChunks = np.array_split(sequencesDf,numBatches)
for i,items in enumerate(sequenceChunks):
    with sequencesTable.batch_writer() as batch:
        for index, row in items.iterrows():
            rowDict = row.to_dict()
            payload = json.loads(json.dumps(rowDict), parse_float=Decimal)
            print(row['genotypeVariantConf'])
            print(type(row['genotypeVariantConf']))
            if isinstance(row['genotypeVariantConf'], float) :
                payload['genotypeVariantConf'] = "NA"
            
            print(payload)
            batch.put_item(Item=payload)

print(sequencesDf.head())