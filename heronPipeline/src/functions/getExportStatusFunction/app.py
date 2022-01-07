import os
from sys import exit, stderr
from datetime import datetime
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

	print(f"Event: {event}, context: {context}")

#   exportArn = event['exportArn']
#   exportArn = os.getenv("EXPORT_ARN")

#   print(f"exportArn: {exportArn}")

  # Create a DynamoDB Client
	# dynamodb = boto3.client('dynamodb', region_name="eu-west-1", config=config)
  
	# ret = dynamodb.describe_export(
  #   ExportArn=exportArn
  # )
  
	return "Hello Wolrd" #ret['ExportStatus']