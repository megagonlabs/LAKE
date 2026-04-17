
import ast
import re
import json
from demo_planners.simple_agents_runnable import *

class Node:
    def __init__(self, tool, inputs, attrs):
        self.tool = tool
        self.inputs = inputs  # list of Node
        self.attrs = attrs    # dict or str or {}

def parse_to_node(ast_node):
    if isinstance(ast_node, ast.Call):
        if not isinstance(ast_node.func, ast.Name):
            raise ValueError("Expected function name")
        tool_name = ast_node.func.id
        
        # Case 1: Already correct (2 args)
        if len(ast_node.args) == 2:
            inputs_ast, attrs_ast = ast_node.args
        
        elif len(ast_node.args) == 1:
            print('Call of functions incorrect : input is being corrected : creating attributes as empty dict')
            inputs_ast, attrs_ast = ast_node.args[0], ast.Dict(keys=[], values=[])
        
        # Case 2: Misformatted: multiple single-element lists + attrs
        elif len(ast_node.args) > 2:
            print('Call of functions incorrect : input is being corrected : merging lists')
            *list_args, attrs_ast = ast_node.args
            if all(isinstance(x, ast.List) for x in list_args):
                # Flatten into a single ast.List
                merged_elts = []
                for lst in list_args:
                    merged_elts.extend(lst.elts)
                inputs_ast = ast.List(elts=merged_elts, ctx=ast.Load())
            else:
                raise ValueError("Unexpected argument format: " + str([ast.dump(x) for x in ast_node.args]))
        
        else:
            raise ValueError("Expected 2 args, got " + str(len(ast_node.args)))
        
        if not isinstance(inputs_ast, ast.List):
            raise ValueError("First argument should be a list")
        inputs = []
        for elt in inputs_ast.elts:
            sub_node = parse_to_node(elt)
            inputs.append(sub_node)
        
        if isinstance(attrs_ast, ast.Dict):
            attrs_str = ast.unparse(attrs_ast)
            attrs = ast.literal_eval(attrs_str)
        elif isinstance(attrs_ast, ast.Constant):
            attrs = attrs_ast.value
        else:
            raise ValueError("Second argument should be dict or string")
        
        return Node(tool_name, inputs, attrs)
    
    elif isinstance(ast_node, ast.List):
        if len(ast_node.elts) == 0:
            # Empty list represents empty input node
            return Node(None, [], {})
        else:
            raise ValueError("Unexpected non-empty list outside of inputs context")
    
    else:
        raise ValueError("Unexpected AST node type")

def print_tree(node, indent=0):
    prefix = '  ' * indent
    if node.tool is None:
        print(prefix + "- empty input")
    else:
        print(prefix + node.tool)
        print(prefix + "- inputs:")
        for inp in node.inputs:
            print_tree(inp, indent + 1)
        print(prefix + "- attrs: " + str(node.attrs))

def parse_chain(chain):
    """
    Takes the chain (list of dicts) as input, parses the last 'OUTPUT' into a tree,
    and prints the tree structure.
    """
    if not chain:
        raise ValueError("Empty chain")
    last_output = chain[-1]["OUTPUT"]
    print('DEBUG CHAIN: ' + last_output)
    # Preprocess to handle #string# as 'string'
    last_output = re.sub(r"#([^#]*)#", r"'\1'", last_output)
    
    # Parse the expression
    expr = ast.parse(last_output, mode='eval').body
    
    # Build the node tree
    root = parse_to_node(expr)
    
    # Print the tree
    print_tree(root)
    
    # Optionally return as dict for further use
    def node_to_dict(n):
        if n.tool is None:
            return {"type": "empty_input"}
        return {
            "tool": n.tool,
            "inputs": [node_to_dict(i) for i in n.inputs],
            "attrs": n.attrs
        }
    return node_to_dict(root)

# Example usage with the provided input
# Assuming the input is given as a list of dicts (the inner list from the query)


# Parse and print the tree
# tree_dict = parse_chain(example_chain)
# print("\nTree as dict:")
# print(json.dumps(tree_dict, indent=2))





# import json

# def run_tool_tree(node, NL_to_RUN, properties=None):
#     """
#     Traverses the tool tree and executes each tool using NL_to_RUN.
    
#     Args:
#         node (dict): The tree node as a dictionary with 'tool', 'inputs', and 'attrs'.
#         NL_to_RUN (callable): Function that executes a tool given its name, inputs, attributes, and properties.
#         properties (dict, optional): Properties to pass to NL_to_RUN. Defaults to empty dict if None.
    
#     Returns:
#         The result of executing the tool or None for empty input nodes.
#     """
#     if properties is None:
#         properties = {}
    
#     # Handle empty input node
#     if node.get("type") == "empty_input":
#         return None
    
#     # Get tool name and attributes
#     tool_name = node["tool"]
#     attrs = node["attrs"]
    
#     # Recursively evaluate inputs
#     inputs = []
#     for input_node in node["inputs"]:
#         input_result = run_tool_tree(input_node, NL_to_RUN, properties)
#         inputs.append(input_result)
    
#     # Execute the tool using NL_to_RUN
#     result = NL_to_RUN(tool_name, inputs, attrs, properties)
#     return result

# def example_usage(tree_dict, NL_to_RUN):
#     """
#     Example function to demonstrate running the tool tree.
    
#     Args:
#         tree_dict (dict): The tool tree as a dictionary.
#         NL_to_RUN (callable): The NL_to_RUN function to execute tools.
    
#     Returns:
#         The final result of the tool execution.
#     """
#     result = run_tool_tree(tree_dict, NL_to_RUN)
#     return result

# # For demonstration, print the tree and simulate running it
# if __name__ == "__main__":
#     # Example tree from previous response (for reference)
#     example_tree = {
#         "tool": "JOIN_2",
#         "inputs": [
#             {
#                 "tool": "NL2SQL",
#                 "inputs": [{"type": "empty_input"}],
#                 "attrs": {
#                     "question": "Select * from jobs where min_experience > 1 and min_experience < 3",
#                     "protocol": "postgres",
#                     "database": "postgres",
#                     "collection": "public"
#                 }
#             },
#             {
#                 "tool": "NL2SQL",
#                 "inputs": [{"type": "empty_input"}],
#                 "attrs": {
#                     "question": "Select * from skills_required_for_job",
#                     "protocol": "postgres",
#                     "database": "postgres",
#                     "collection": "public"
#                 }
#             }
#         ],
#         "attrs": {
#             "join_on_table1": "unique_job_id",
#             "join_on_table2": "unique_job_id",
#             "join_type": "inner",
#             "join_suffix": ["_jobs", "_skills"],
#             "keep_keys": "both"
#         }
#     }
    
#     # Mock NL_to_RUN for demonstration (replace with actual implementation)
#     def mock_NL_to_RUN(tool, inp, attributes, properties):
#         print(f"Executing {tool} with inputs {inp} and attributes {attributes}")
#         return f"Result of {tool}"
    
#     # Run the tree
#     result = example_usage(example_tree, mock_NL_to_RUN)
#     print("Final result:", result)


import asyncio
from concurrent.futures import ThreadPoolExecutor
import json
import time

async def run_tool_tree(node, NL_to_RUN, properties=None, executor=None):
    """
    Traverses the tool tree, executes tools concurrently, and builds an output tree.
    
    Args:
        node (dict): The tree node as a dictionary with 'tool', 'inputs', and 'attrs'.
        NL_to_RUN (callable): Function that executes a tool given its name, inputs, attributes, and properties.
        properties (dict, optional): Properties to pass to NL_to_RUN. Defaults to empty dict if None.
        executor (ThreadPoolExecutor, optional): Executor for running synchronous NL_to_RUN in async context.
    
    Returns:
        tuple: (result, output_node)
            - result: The result of executing the tool or None for empty input nodes.
            - output_node: A dictionary with 'tool', 'inputs', 'attrs', and 'result'.
    """
    if properties is None:
        properties = {}
    
    # Handle empty input node
    if node.get("type") == "empty_input":
        # Most tools in this stack expect empty input as `[[]]` (one empty table).
        return [[]], {"type": "empty_input", "result": [[]]}
    
    # Get tool name and attributes
    tool_name = node["tool"]
    attrs = node["attrs"]
    
    # Log start of tool execution
    print(f"[{time.strftime('%H:%M:%S')}] Starting {tool_name}")
    
    # Recursively evaluate inputs concurrently
    input_tasks = [run_tool_tree(input_node, NL_to_RUN, properties, executor) for input_node in node["inputs"]]
    input_results = await asyncio.gather(*input_tasks, return_exceptions=True)
    

    inputs = []
    input_output_nodes = []
    for item in input_results:
        if isinstance(item, Exception):
            raise item
        result, output_node = item
        inputs.append(result)
        input_output_nodes.append(output_node)

    
    # Execute the tool using NL_to_RUN
    if executor:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, lambda: NL_to_RUN(tool_name, inputs, attrs, properties))
    else:
        result = NL_to_RUN(tool_name, inputs, attrs, properties)
    
    # Create output node
    output_node = {
        "tool": tool_name,
        "inputs": input_output_nodes,
        "attrs": attrs,
        "result": result
    }
    
    print(f"[{time.strftime('%H:%M:%S')}] Completed {tool_name}")
    return result, output_node

async def example_usage(tree_dict, NL_to_RUN):
    """
    Runs the tool tree with concurrent subtree roots and returns the result and output tree.
    
    Args:
        tree_dict (dict): The tool tree as a dictionary.
        NL_to_RUN (callable): The NL_to_RUN function to execute tools.
    
    Returns:
        tuple: (result, output_tree)
            - result: The final result of the tool execution.
            - output_tree: The tree of outputs mirroring the input tree.
    """
    with ThreadPoolExecutor() as executor:
        result, output_tree = await run_tool_tree(tree_dict, NL_to_RUN, executor=executor)
    return result, output_tree

# For demonstration, print the tree and simulate running it
