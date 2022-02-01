"""
Checks a sample if it matches PHE defined recipes for VOC/VUIs.  Outputs to stdout
a tab delimited list of the following:

- PHE name for the matching VOC/VUI.  "none" if no match.  "multiple" if multiple matches.
- pangolin name for the matching VOC/VUI.  "none" if no match.  "multiple" if multiple matches.
- confidence of the match.   "NA" if no match.  "multiple" if multiple matches.
- current time on system

Logs debugging information to stderr

"""
from csv import reader
from argparse import ArgumentParser
from yaml import full_load as load_yaml
from datetime import datetime
from sys import exit, stderr
import logging
from recipe_graph import RecipeDirectedGraph
from typing import Tuple

WUHAN_REFERENCE_LENGTH = 29903


logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# create console handler and set level to debug
ch = logging.StreamHandler(stderr)
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
# add formatter to ch
ch.setFormatter(formatter)




def get_recipe_match_confidence(recipe: dict, sequence: str, cached_results: dict) -> str:
    """
    Calculate the confidence of a sample matching a given PHE VOC/VUI recipe.
    PHE VOC/VUI recipes have a list of defining mutations and 
    different confidence thresholds based on the number of defining mutations that are found
    in the sample.  A PHE recipe typically defines only a single lineage,
    and can depend on other PHE recipes to define required mutations from the ancestral lineages.

    Assumes that a recipe can only depend on a single other recipe.
    We use child to refer to a recipe that depends on another recipe,
    and parent to refer to the other recipe being depended on by the recipe.
    Ancestors are the chain of recipes being depended on by a child. 

    A child returns "confirmed" confidence  if it passes "confirmed" threshold of it's own mutation definition,
        AND all it's ancestors return "confirmed" or "probable".
    A child returns "probable" confidence if it only passes the "probable" threshold of it's own mutation definition,
        AND all it's ancestors return "confirmed" or "probable".
    A child returns "NA" confidence if it fails all thresholds of it's own mutation definition OR
        any of it's ancestors return "NA"
    
    NB:   If the recipe depends on another recipe, then calls get_recipe_match_confidence()
    recursively. Does not check for cyclic dependencies in the recipes.
    If you notice that this method is taking forever, check that there isn't a cycle in the
    recipe dependencies causing an infinite recursion loop.

    See more details on PHE recipes at https://github.com/phe-genomics/variant_definitions

    Parameters:
    -------------
    recipe : dict
        a dict representation of a single PHE recipe.
        Expects the recipe dict to contain items:
            - unique-id (str): the recipe name
            - variants (list):  list of SNPs, MNPs, deletions, insertions that define the lineage.
                Each mutation will have nested items:
                - one-based-reference-position (int):  reference position, 1 based
                - type:  one of [SNP, MNP, deletion, insertion]
                - reference-base:  ref base if type=SNP, contiguous ref bases if type=MNP, ref base
                    before insertion if type=insertion, ref base before deletion and deleted ref bases if type=deletion
                - variant-base:  alt base if type=SNP, contiguous alt bases if type=MNP,
                    ref base before deletion if type=deletion, ref base before insertion followed by inserted bases if 
                    type=insertion
                - special (bool):  only if the mutation is absolutely required
            - calling-definition (dict):   dict of how many mutations are required
                for confirmed or probable confidence
            - belongs-to-lineage (dict):  nested dict containing item {"PANGO" => pangolin lineage}
            - phe-label (str):  PHE name for the VOC/VUI
            - requires (str):  the name of the recipe that the current recipe depends on.  Can be missing if no dependencies.
    
    sequence: str
        Wuhan aligned sequence of sample.  Deletions with respect to the reference must be padded with "-",
        insertions with respect to the reference must be excised.

    cached_results: dict
        Hack to keep track of previous results in case we need to recursively call 
        get_recipe_match_confidence() to get the confidence of nested recipes.
        Should have format  {recipe_name => confidence}
    
    Returns:  str
    ------------
    The confidence of the match for the recipe, taking into account all ancestral recipes if any

    """

    recipe_name = recipe["unique-id"]
    if recipe_name in cached_results:
        logger.debug("Using cached results: " + cached_results[recipe_name])
        return cached_results[recipe_name]

    alt_match = 0
    ref_match = 0

    #if a "special mutation", e.g. E484K is required, but not present, this will be flipped to False
    special_mutations = True

    # Keep track of matched variants for logging
    log_alt_match = []
    log_ref_match = []
    log_wrong_alt = []

    pango_alias = "none"
    phe_label = "none"
    confidence = "NA"

    req_recipe_confidence = None
    

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

    pango_alias = recipe['belongs-to-lineage']['PANGO']
    phe_label = recipe['phe-label']

    if (special_mutations and 
            alt_match >= calling_definition['confirmed']['mutations-required'] and 
            ref_match <= calling_definition['confirmed']['allowed-wildtype']):
        confidence = "confirmed"
    elif ('probable' in calling_definition and 
            special_mutations and 
            alt_match >= calling_definition['probable']['mutations-required'] and 
            ref_match <= calling_definition['probable']['allowed-wildtype']):
        confidence = "probable"
    
    overall_confidence = confidence
    if "requires" in recipe and confidence in ["confirmed", "probable"]:
        req_recipe_name = recipe["requires"]
        req_recipe_pango_alias = recipes[req_recipe_name]['belongs-to-lineage']['PANGO']
        req_recipe_phe_label = recipes[req_recipe_name]['phe-label']

        logger.debug(f"Checking required recipe {req_recipe_name} - {req_recipe_pango_alias} " + 
                      f"of dependent recipe {recipe_name} - {pango_alias} ")

        req_recipe_confidence = get_recipe_match_confidence(
            recipe=recipes[req_recipe_name], 
            sequence=sequence,
            cached_results=cached_results)
  
        logger.debug(f"Required recipe pango: {req_recipe_pango_alias}" + 
                    f", confidence: {req_recipe_confidence}" + 
                    f", phe-label: {req_recipe_phe_label}" + 
                    f" for reciped recipe {req_recipe_name} - {req_recipe_pango_alias} " + 
                    f" of dependent recipe {recipe_name} - {pango_alias} ")

        if req_recipe_confidence not in ["confirmed", "probable"]:
            overall_confidence = "NA"

    if confidence in ["confirmed", "probable"]:
        
        logger.debug(f"Matched pango: {pango_alias} " + 
                     f", confidence: {confidence} " + 
                     f", overall-confidence: {overall_confidence} " + 
                     f", phe-label: {phe_label}.  " )
        logger.debug("Alt matches:  " + ", ".join(log_alt_match))
        logger.debug("Ref matches:  " + ", ".join(log_ref_match))
        logger.debug("Wrong Alt:  " + ", ".join(log_wrong_alt))
    
    return overall_confidence


def find_all_matching_recipes(recipes: dict, sequence: str) -> Tuple[str, str, str]:
    """
    Traverse through all PHE VOC/VUI recipes and find all matches.

    If a sample matches multiple PHE recipes, and
    and the recipes are not along the same branch in the recipe dependency graph,
    then the sample is marked as matching "multiple" recipes.

    If the sample matches multiple PHE lineage recipes, and
    the lineages are related along the same tree branch,
    (EG AY.4.2 is a child of B.1.617.2),
    then the sample is marked as the lowest lineage along the branch.

    Parameters:
    --------------------
    recipes : dict
        {recipe_name => recipe_dict}
        Load the dict of recipes from phe_recipes.yaml.
    
    sequence: str
        wuhan aligned sequence of sample.  Deletions padded with "-".  Insertions removed.

    Returns:  tuple (str, str, str)
    ---------------------------------
        - matched_recipe_phe_label: str
            PHE name for the VOC/VUI.  "none" if no match.  "multiple" if multiple matches.
        - matched_recipe_pango_alias: str
            pangolin name for the VOC/VUI.  "none" if no match.  "multiple" if multiple matches.
        - matched_confidence: str
            confidence of the match.   "NA" if no match.  "multiple" if multiple matches.
    """
    
    # traverse the recipes and cache any matching recipes and 
    # associated confidence in dict matched_recipe_name_to_conf
    matched_recipe_name_to_conf = {}
    for recipe in recipes.values():
        
        confidence = get_recipe_match_confidence(
            recipe=recipe, 
            sequence=sequence,
            cached_results=matched_recipe_name_to_conf)

        if confidence != "NA":
            recipe_name = recipe["unique-id"]
            matched_recipe_name_to_conf[recipe_name] = confidence
    
    # If there are multiple matching recipes, but they are all recipes for related lineages
    # along the same branch in the lineage tree, then
    # we return the lineage recipe for leaf-most lineage.
    # If the matching lineages are from different branches in the lineage tree,
    # then we mark the sample as "multiple", indicating that there are
    # multiple conflicting lineage matches
    if len(matched_recipe_name_to_conf.keys()) > 1:
        matched_recipes = [recipes[recipe_name] for recipe_name in matched_recipe_name_to_conf.keys()]
        matched_recipe_graph = RecipeDirectedGraph(matched_recipes)
        if matched_recipe_graph.is_single_branch():
            leaf_recipe_name = matched_recipe_graph.get_leaf_name()
            leaf_recipe = recipes[leaf_recipe_name]
            matched_recipe_pango_alias = leaf_recipe['belongs-to-lineage']['PANGO']
            matched_recipe_phe_label = leaf_recipe['phe-label']
            matched_confidence = matched_recipe_name_to_conf[leaf_recipe_name] 
        else:
            matched_recipe_pango_alias = "multiple"
            matched_recipe_phe_label = "multiple"
            matched_confidence = "multiple"
            logger.warning("Multiple matches " + str(matched_recipe_graph) )
    elif len(matched_recipe_name_to_conf.keys()) == 1:
        matched_recipe_name = list(matched_recipe_name_to_conf.keys())[0]
        matched_recipe = recipes[matched_recipe_name]
        matched_recipe_pango_alias = matched_recipe['belongs-to-lineage']['PANGO']
        matched_recipe_phe_label = matched_recipe['phe-label']
        matched_confidence = matched_recipe_name_to_conf[matched_recipe_name] 
    else:
        matched_recipe_pango_alias = 'none'
        matched_recipe_phe_label = 'none'
        matched_confidence = 'NA'

    return matched_recipe_phe_label, matched_recipe_pango_alias, matched_confidence


if __name__ == "__main__":

    parser = ArgumentParser(description='Genotype an aligned sequence on specified variants of interest')
    parser.add_argument('fasta_filename', help="Single sample fasta, wuhan aligned")
    parser.add_argument('genotype_recipe_filename', help="Concatenated YAML of PHE VOC/VUI recipes")
    parser.add_argument("--verbose", help="increase output verbosity",
                        action="store_true")

    args = parser.parse_args()

    if args.verbose:
        ch.setLevel(logging.DEBUG)
    else:
        ch.setLevel(logging.WARN)
    # add ch to logger
    logger.addHandler(ch)

    matched_recipe_pango_alias = "none"
    matched_recipe_phe_label = "none"
    matched_confidence = "NA"

    logger.debug("Processing " + args.fasta_filename)
    with open(args.fasta_filename) as fasta_file:
        header = fasta_file.readline()
        if header[0] != ">":
            logger.error("Error with fasta header line. "+header[0])
            exit(-1)
        sequence = fasta_file.readline().rstrip()
        if len(sequence) != WUHAN_REFERENCE_LENGTH:
            logger.error("Error, sequence doesn't match Wuhan reference length.")
            exit(-1)

    with open(args.genotype_recipe_filename) as genotype_recipe_file:
        recipes = load_yaml(genotype_recipe_file)
    
    (matched_recipe_phe_label, 
    matched_recipe_pango_alias, 
    matched_confidence) = find_all_matching_recipes(recipes=recipes, sequence=sequence)

    print(matched_recipe_phe_label, matched_recipe_pango_alias, matched_confidence, datetime.now(), sep="\t")



# print(matched_recipe_phe_label, matched_recipe_pango_alias, matched_confidence, datetime.now(), sep="\t")
