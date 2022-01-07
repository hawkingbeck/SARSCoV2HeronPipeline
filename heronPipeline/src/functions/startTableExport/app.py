from csv import reader
import os
from argparse import ArgumentParser
from yaml import full_load as load_yaml
from datetime import datetime
from sys import exit, stderr
import uuid
import json
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

  heronBucketName = os.getenv("HERON_BUCKET")
  heronMutationsTableArn = os.getenv("HERON_MUTATIONS_TABLE")

  print(f"Bucket: {heronBucketName}")
  print(f"Table: {heronMutationsTableArn}")

  return "Hello World"