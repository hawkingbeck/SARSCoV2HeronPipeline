import os
from sys import exit, stderr
from datetime import datetime, timedelta
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config
from boto3.dynamodb.conditions import Key


config = Config(
   retries = {
      'max_attempts': 10,
      'mode': 'standard'
   }
)

def lambda_handler(event, context):
  print(f"Input event: {event}")
  heronBucketName = event['heronBucket']
  heronTableArn = event["heronTable"]
  exportKey = event['exportKey']

  print(f"Bucket: {heronBucketName}")
  print(f"Table: {heronTableArn}")
  print(f"export Key: {exportKey}")

  yesterday = datetime.today() - timedelta(days=0)
  exportDate = datetime(yesterday.year, yesterday.month, yesterday.day, 0, 0, 0).timestamp()
  exportPartition = datetime.strftime(datetime.now(), "%Y-%m-%d")

  # Create a DynamoDB Client
  dynamodb = boto3.client('dynamodb', region_name="eu-west-1", config=config)
  ret = dynamodb.export_table_to_point_in_time(
    TableArn=heronTableArn,
    ExportTime=exportDate,
    S3Bucket=heronBucketName,
    S3Prefix=f"{exportKey}/{exportPartition}",
    S3SseAlgorithm='AES256',
    ExportFormat='DYNAMODB_JSON'
  )
  
  exportArn = ret['ExportDescription']['ExportArn']
  exportUUID = os.path.basename(ret['ExportDescription']['ExportArn'])
  exportState = ret['ExportDescription']['ExportStatus']
  s3Prefix = ret['ExportDescription']['S3Prefix']
  resultS3Prefix = f"{s3Prefix}/AWSDynamoDB/{exportUUID}/exported.csv"

	# print(f"Ret: {ret}")

  return {'exportArn': exportArn, 'exportUUID': exportUUID, 'exportState': exportState, 's3Prefix': s3Prefix, 'exportKey': exportKey, 'resultS3Prefix': resultS3Prefix}