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
    """Lambda function to generate the Map input state for LQP Run Base

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
    
    batches = list()
    for b in range(3):
      partitions = list()
      for i in range(1,41):
        partitions.append({"partition": str( (b*40) + i)})
      batch = dict({'partitions': partitions})
      batches.append(batch)
      
    return {'batches': batches}




    
      

    

