import yaml
from yaml import full_load as load_yaml

localRecipeFilename = "./phe-recipes.yml"

with open(localRecipeFilename) as genotype_recipe_file:
  recipes = load_yaml(genotype_recipe_file)

for recipe in recipes.values():
  recipe_name = recipe["unique-id"]
  print(f"Processing Recipe: {recipe_name}")
