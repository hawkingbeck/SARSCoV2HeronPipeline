from csv import reader
from argparse import ArgumentParser
from yaml import full_load as load_yaml
from datetime import datetime
from sys import exit, stderr
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler(stderr)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)


wuhan_reference_length = 29903

matched_recipe_pango_alias = "none"
matched_recipe_phe_label = "none"
matched_confidence = "NA"

parser = ArgumentParser(description='Genotype an ivar output on specified variants of interest')
parser.add_argument('fasta_filename') #must be Wuhan aligned.
parser.add_argument('genotype_recipe_filename')
parser.add_argument("--verbose", help="increase output verbosity",
                    action="store_true")

args = parser.parse_args()

if args.verbose:
    ch.setLevel(logging.DEBUG)
else:
    ch.setLevel(logging.WARN)
# add ch to logger
logger.addHandler(ch)


with open(args.fasta_filename) as fasta_file:
    header = fasta_file.readline()
    if header[0] != ">":
        logger.error("Error with fasta header line. "+header[0])
        exit(-1)
    sequence = fasta_file.readline().rstrip()
    if len(sequence) != wuhan_reference_length:
        logger.error("Error, sequence doesn't match Wuhan reference length.")
        exit(-1)

with open(args.genotype_recipe_filename) as genotype_recipe_file:
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

        logger.debug("Matched pango {} confidence {} phe-label {}.  {}".format(matched_recipe_pango_alias, confidence, matched_recipe_phe_label, args.fasta_filename))
        logger.debug("Alt matches:  " + ", ".join(log_alt_match))
        logger.debug("Ref matches:  " + ", ".join(log_ref_match))
        logger.debug("Wrong Alt:  " + ", ".join(log_wrong_alt))


print(matched_recipe_phe_label, matched_recipe_pango_alias, matched_confidence, datetime.now(), sep="\t")
