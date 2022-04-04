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

##############################################
# Read environment variables
##############################################
# the date string used to partitioin data for this run
dateString = os.getenv('DATE_PARTITION')
# The root of the EFS attached to the container
sampleDataRoot = os.getenv('SEQ_DATA_ROOT')

# Remove all files under this path for each run of the state machine
efsFolder = f"{sampleDataRoot}/{dateString}"

# Get a list of directories that we wish to delete
directoryList = os.listdir(sampleDataRoot)

for directory in directoryList:
  shutil.rmtree(directory, ignore_errors=False, onerror=None)

