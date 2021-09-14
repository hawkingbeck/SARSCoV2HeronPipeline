import pandas as pd
import numpy as np
import os
from datetime import datetime
import boto3
from botocore.config import Config
from boto3.dynamodb.conditions import Key, Attr
import json
import math



def lambda_handler(event, context):
    """Lambda function which checks if the meta data for the current day is available or not

    Parameters
    ----------
    event: dict, required
        Input event to the Lambda function

    context: object, required
        Lambda Context runtime methods and attributes

    Returns
    ------
        dict: array with the meta data state
    """


    lqpDataRoot = os.getenv("LQP_DATA_ROOT")
    heronBucketName = os.getenv("HERON_SAMPLES_BUCKET")
    dateString = event["date"]


    # Check that metadata files exist
    metaDataReady = True
    fileNames = ["base_changes", "ambiguous", "root"]
    for filename in fileNames:
      pathToCheck = f"{lqpDataRoot}/{dateString}/{filename}"
      if os.path.exists(pathToCheck) == False:
        metaDataReady = False
        break
    

    return {'metaDataReady': metaDataReady}




    
      

    

