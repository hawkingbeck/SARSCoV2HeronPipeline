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

# Read the inputs that we require from the event
# These will S3 paths that require download prior to using
# fastaFileS3Key = event['consensusFastaPath']
# fastaFileS3Key = os.getenv['consensusFastaPath']
genotypeRecipeS3Key = os.getenv('RECIPE_FILE_PATH')
heronBucketName = os.getenv("HERON_SAMPLES_BUCKET")
heronSequencesTableName = os.getenv("HERON_SEQUENCES_TABLE")
messageListS3Key = os.getenv('MESSAGE_LIST_S3_KEY')
messageListLocalFilename = "/tmp/messageList.json"
localRecipeFilename = f"/tmp/{str(uuid.uuid4())}.recipe"

#create the AWS client resources we need for this execution
s3 = boto3.resource('s3', region_name='eu-west-1')
bucket = s3.Bucket(heronBucketName)
dynamodb = boto3.resource('dynamodb', region_name="eu-west-1", config=config)
sequencesTable = dynamodb.Table(heronSequencesTableName)

# Download the receipe file that we need for each sequence



callDate = int(datetime(datetime.now().year, datetime.now().month, datetime.now().day, 0, 0, 0).timestamp())

# Download the message file that contains the references to all the sequences that we need to process
bucket.download_file(messageListS3Key, messageListLocalFilename)
# Download the recipe file that we need to assign variants from
bucket.download_file(genotypeRecipeS3Key, localRecipeFilename)

with open(messageListLocalFilename) as messageListFile:
   messageList = json.load(messageListFile)

for message in messageList:
  # Download the fasta file for this message
  print(f'Message: {message["consensusFastaPath"]}')
  # Download the consensus fasta
  consensusFastaKey = message["consensusFastaPath"]
  consensusFastaHash = message['seqHash']

  sequenceLocalFilename = f"/tmp/seq_{consensusFastaHash}_.json"

  try:
    bucket.download_file(consensusFastaKey, sequenceLocalFilename)
  except:
    print(f"File not found: {consensusFastaKey}")
    sampleLocalFilename = None

  alignedFasta = None
  with open(sequenceLocalFilename, "r") as fasta:
    seqData = json.load(fasta)
    alignedFasta = seqData['aligned']


  # Download the files as unique local filenames to avoid any clashes with /tmp directory
  localFastaFilename = f"/tmp/{str(uuid.uuid4())}.fasta"

  with open(localFastaFilename, "w") as fasta:
    fasta.write(alignedFasta)

  wuhan_reference_length = 29903
  matched_recipe = "none"
  matched_confidence = "NA"
  matched_recipe_phe_label = "NA"
  matched_recipe_pango_alias = "none"



  with open(localFastaFilename) as fasta_file:
    header = fasta_file.readline()
    if header[0] != ">":
        print("Error with fasta header line. "+header[0],file=stderr)
        exit(-1)
    sequence = fasta_file.readline().rstrip()
    if len(sequence) != wuhan_reference_length:
        print("Error, sequence doesn't match Wuhan reference length.",file=stderr)
        exit(-1)

  with open(localRecipeFilename) as genotype_recipe_file:
    recipes = load_yaml(genotype_recipe_file)


  for recipe in recipes.values():
    alt_match = 0
    ref_match = 0

    #if a "special mutation", e.g. E484K is required, but not present, this will be flipped to False
    special_mutations = True

    # Keep track of matched variants for logging
    log_alt_match = []
    log_ref_match = []
    log_wrong_alt = []

    for lineage_mutation in recipe['variants']:
        pos = int(lineage_mutation['one-based-reference-position'])-1
        if lineage_mutation['type'] == "MNP":
            size = len(lineage_mutation['reference-base'])
            seq_val = sequence[pos:pos+size]
        elif lineage_mutation['type'] == "SNP":
            seq_val = sequence[pos]
        else:
            #not considering indels at present
            continue

        log_is_special = "_spec" if "special" in lineage_mutation else ""

        if seq_val == lineage_mutation['variant-base']:
            alt_match += 1

            log_alt_match.append("{}{}{}{}".format(
                lineage_mutation['reference-base'],
                lineage_mutation['one-based-reference-position'],
                seq_val,
                log_is_special
                ))

        elif seq_val == lineage_mutation['reference-base']:
            ref_match += 1
            if "special" in lineage_mutation:
                special_mutations = False

            log_ref_match.append("{}{}{}".format(
                lineage_mutation['reference-base'],
                lineage_mutation['one-based-reference-position'],
                log_is_special))
        else:
            if "special" in lineage_mutation:
                special_mutations = False

            log_wrong_alt.append("{}{}{}/{}{}".format(
                lineage_mutation['reference-base'],
                lineage_mutation['one-based-reference-position'],
                lineage_mutation['variant-base'],
                seq_val,
                log_is_special))

    calling_definition = recipe['calling-definition']
    confidence = "NA"
    if special_mutations and alt_match >= calling_definition['confirmed']['mutations-required'] and ref_match <= calling_definition['confirmed']['allowed-wildtype']:
        confidence = "confirmed"
    elif 'probable' in calling_definition and special_mutations and alt_match >= calling_definition['probable']['mutations-required'] and ref_match <= calling_definition['probable']['allowed-wildtype']:
        confidence = "probable"

    if confidence != "NA":
        if matched_recipe_pango_alias == "none":
            matched_recipe_pango_alias = recipe['belongs-to-lineage']['PANGO']
            matched_recipe_phe_label = recipe['phe-label']
            matched_confidence = confidence

        else:
            matched_recipe_pango_alias = "multiple"
            matched_recipe_phe_label = "multiple"
            matched_confidence = "multiple"


  print(matched_recipe, matched_confidence, matched_recipe_phe_label, datetime.now(), sep="\t")

  # Upsert the record for the sequence
  response = sequencesTable.query(
        KeyConditionExpression=Key('seqHash').eq(consensusFastaHash)
      )

  if 'Items' in response:
    if len(response['Items']) == 1:
      item = response['Items'][0]
      item['processingState'] = 'aligned'
      ret = sequencesTable.update_item(
          Key={'seqHash': consensusFastaHash},
          UpdateExpression="set genotypeVariant=:v, genotypeVariantConf=:c, genotypeCallDate=:d, genotypeProfile=:p",
          ExpressionAttributeValues={
            ':v': matched_recipe,
            ':c': matched_confidence,
            ':d': callDate,
            ':p': matched_recipe_phe_label
          }
        )