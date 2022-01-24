from dataclasses import dataclass
from typing import List



@dataclass
class Node:
    """
    Assume that a recipe can only require a single parent recipe.
    However, a recipe can be required by multiple child recipes.
    """
    name: str
    child_names: List[str]
    parent_name: str
        
        
class RecipeDirectedGraph:
    def __init__(self, recipes: List[dict]):
        """
        Parameters:
        --------------
        recipes : list of recipe dicts
            Each recipe dict is defined by a PHE recipe YAML.

        Returns:  
        -------------
            An directed graph representation of the recipes.
            Two recipes are connected if a recipe
            has a 'requires' tag in the PHE recipe YAML that points
            to the other recipe.

            Internally, the graph is represented by a dict {node name => Node},
            A Node object stores the name of it's parent node and names of it's child nodes,
            but not the child Node and parent Node objects themselves.
            The collection of node objects is only stored in the graph dict.

            A child node is a recipe that "requires" another recipe
            and a parent node is a recipe that is required by another recipe.
            There should be a directed edge between each child and parent.

            Recipes that do not require another recipe and 
            are not required by other recipes will be unconnected nodes in the graph.
        """
        self.graph = {}
        for recipe in recipes:
            recipe_name = recipe["unique-id"]
            if recipe_name not in self.graph:
                self.graph[recipe_name] = Node(name=recipe_name, child_names=[], parent_name=None)
            
            recipe_node = self.graph[recipe_name]
                
            if "requires" in recipe:
                parent_recipe_name = recipe["requires"]
                if parent_recipe_name not in self.graph:
                    self.graph[parent_recipe_name] = Node(name=parent_recipe_name, child_names=[], parent_name=None)
                
                parent_recipe_node = self.graph[parent_recipe_name]
                recipe_node.parent_name = parent_recipe_name
                parent_recipe_node.child_names.append(recipe_name)
    

    def is_single_branch(self):
        """
        Returns: boolean
        -----------------
            Whether all nodes in the graph are connected in a single branch, 
            forming a single path from root to leaf, and having only 1 leaf.

            If there are recipes in the graph that represent splitting branches
            in a tree, then is_single_branch() will return false.
        """

        if len(self.graph.keys()) < 2:
            raise ValueError("Can only calculate leaf in graph with at least 2 nodes")

        leaf_count = 0
        is_connected = True
        for node in self.graph.values():
            # Disconnected node in graph
            if node.parent_name is None and len(node.child_names) == 0:
                is_connected = False
                break
            
            # Leaf node
            if node.parent_name is not None and len(node.child_names) == 0:
                leaf_count += 1
    
        return is_connected and leaf_count == 1
        

    def get_leaf_name(self):
        """
        Returns:  str
        -----------------
            Returns the name of the first leaf encountered or None if there are no leafs.
            A leaf is a node that has a parent node but no child nodes.
            Does not care if the graph is disconnected.

        Raises:
        -----------------
        ValueError
            If the graph has less than 2 nodes
        """
        if len(self.graph.keys()) < 2:
            raise ValueError("Can only calculate leaf in graph with at least 2 nodes")

        for node in self.graph.values():
            if node.parent_name is not None and len(node.child_names) == 0:
                return node.name

        return None
        
    
    def __str__(self):
        graph_str = ""
        for node_name, node in self.graph.items():
            child_str = ", ".join(node.child_names)
            graph_str += f"{node_name}->[{child_str}]\n"
        return graph_str

