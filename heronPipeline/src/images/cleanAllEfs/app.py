import os
import os.path
import subprocess
import pandas as pd
import sys
import shutil
from shutil import copyfile
import uuid
import pandas as pd
from datetime import datetime
import argparse
import numpy as np
import time
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


sampleDataRoot = "/mnt/efs0/seqData"
# Get a list of directories that we wish to delete
directoryList = os.listdir(sampleDataRoot)

for directory in directoryList:
  directory = f"{sampleDataRoot}/{directory}"
  print(f"Directory 1: {directory}")
  shutil.rmtree(directory, ignore_errors=False, onerror=None)
  
directoryList = os.listdir(sampleDataRoot)

for directory in directoryList:
  print(f"Directory 2: {directory}")


