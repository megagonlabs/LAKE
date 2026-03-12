
import ast
import re
import json
from demo_planners.simple_agents_runnable import *

class Node:
    def __init__(self, tool, inputs, attrs):
        self.tool = tool
        self.inputs = inputs  # list of Node
        self.attrs = attrs    # dict or str or {}

# def parse_to_node(ast_node):
#     if isinstance(ast_node, ast.Call):
#         if not isinstance(ast_node.func, ast.Name):
#             raise ValueError("Expected function name")
#         tool_name = ast_node.func.id
#         if len(ast_node.args) != 2:
#             raise ValueError("Expected exactly 2 arguments for tool call"+str([str(x) for x in ast_node.args]))
        
#         inputs_ast = ast_node.args[0]
#         attrs_ast = ast_node.args[1]
        
#         if not isinstance(inputs_ast, ast.List):
#             raise ValueError("First argument should be a list")
        
import ast

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

# # Example usage (assuming NL_to_RUN and the tree are provided)
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
if __name__ == "__main__":



    # Example tree with two subtrees
    # example_tree = {
    #     "tool": "JOIN_2",
    #     "inputs": [
    #         {
    #             "tool": "NL2SQL",
    #             "inputs": [{"type": "empty_input"}],
    #             "attrs": {
    #                 "question": "Select * from jobs where min_experience > 1 and min_experience < 3",
    #                 "protocol": "postgres",
    #                 "database": "postgres",
    #                 "collection": "public"
    #             }
    #         },
    #         {
    #             "tool": "NL2SQL",
    #             "inputs": [{"type": "empty_input"}],
    #             "attrs": {
    #                 "question": "Select * from skills_required_for_job",
    #                 "protocol": "postgres",
    #                 "database": "postgres",
    #                 "collection": "public"
    #             }
    #         }
    #     ],
    #     "attrs": {
    #         "join_on_table1": "unique_job_id",
    #         "join_on_table2": "unique_job_id",
    #         "join_type": "inner",
    #         "join_suffix": ["_jobs", "_skills"],
    #         "keep_keys": "both"
    #     }
    # }

    example_chain=[
        
            {
            "TASK": "REFORMULATE",
            "REASONING": "The original query is a question. Reformulating it into a statement will make it clearer and more direct for processing.",
            "OUTPUT": "#Find jobs with a duration of more than 1 year but less than 3 years, and require Python skills.#"
            },
            {
            "TASK": "DATA CHECK",
            "REASONING": "Do we have job duration information in the database? Yes, we have 'min_experience' in the 'jobs' table. Does it include information on skills? Yes, the 'skills_required_for_job' table includes 'skill_required'. Is it in the correct format? Yes, but we need to ensure the duration is between 1 and 3 years. Is it usable without additional operations? Yes, but we need to join the tables to match jobs with their required skills.",
            "OUTPUT": "#Find jobs with min_experience between 1 and 3 years and require Python skills.#"
            },
            {
            "TASK": "TOOL CHECK",
            "REASONING": "What is the correct tool to use? We need to gather data from the database. We can use NL2SQL to convert the natural language query into a SQL query. Why is this tool appropriate for the task? NL2SQL can directly query the database to retrieve job durations and their required skills based on a natural language input.",
            "OUTPUT": "NL2SQL([[]],#jobs with min_experience between 1 and 3 years and require Python skills#)"
            },
            {
            "TASK": "TOOL INPUT CHECK",
            "REASONING": "Can the tool operate given what is provided to it? Yes - NL2SQL operates on the database directly without input data. Is it usable without additional operations? No, we need to join the 'jobs' table with the 'skills_required_for_job' table to filter by Python skills.",
            "OUTPUT": "NL2SQL([[]],#jobs with min_experience between 1 and 3 years and require Python skills#)"
            },
            {
            "TASK": "TOOL INPUT TO ATTRIBUTES",
            "REASONING": "We need to convert the attributes to the correct dictionary format. We need the attribute question. We make sure to give the correct names as shown in data. We will give it the value ``Select 'unique_job_id', 'min_experience' from 'jobs' where 'min_experience' > 1 and 'min_experience' < 3'''. For the protocol, we use postgres. For database, we use postgres. For collection, we use public. The context can be empty as we don't have additional details to provide.",
            "OUTPUT": "NL2SQL([[]],{'question':\"Select 'unique_job_id', 'min_experience' from 'jobs' where 'min_experience' > 1 and 'min_experience' < 3\", 'protocol':'postgres','database':'postgres', 'source':'postgres_example','collection':'public'})"
            },
            {
            "TASK": "TOOL CHECK",
            "REASONING": "What is the correct tool to use? We need to join the results from NL2SQL with the 'skills_required_for_job' table to filter by Python skills. We use JOIN_2 for this task. Why is this tool appropriate for the task? JOIN_2 can combine data from two tables based on a common key, allowing us to filter jobs by required skills.",
            "OUTPUT": "JOIN_2([NL2SQL([[]],{'question':\"Select 'unique_job_id', 'min_experience' from 'jobs' where 'min_experience' > 1 and 'min_experience' < 3\", 'protocol':'postgres','database':'postgres', 'source':'postgres_example','collection':'public'}), NL2SQL([[]],{'question':\"Select 'unique_job_id', 'skill_required' from 'skills_required_for_job' where 'skill_required' = 'Python'\", 'protocol':'postgres','database':'postgres', 'source':'postgres_example','collection':'public'})], #join on 'unique_job_id'#)"
            },
            {
            "TASK": "TOOL INPUT CHECK",
            "REASONING": "Can the tool operate given what is provided to it? Yes - JOIN_2 operates on two input tables, joining them on a common key. Is it usable without additional operations? Yes, the data from NL2SQL provides the necessary columns for JOIN_2 to process.",
            "OUTPUT": "JOIN_2([NL2SQL([[]],{'question':\"Select 'unique_job_id', 'min_experience' from 'jobs' where 'min_experience' > 1 and 'min_experience' < 3\", 'protocol':'postgres','database':'postgres', 'source':'postgres_example','collection':'public'}), NL2SQL([[]],{'question':\"Select 'unique_job_id', 'skill_required' from 'skills_required_for_job' where 'skill_required' = 'Python'\", 'protocol':'postgres','database':'postgres', 'source':'postgres_example','collection':'public'})], #join on 'unique_job_id'#)"
            },
            {
            "TASK": "TOOL INPUT TO ATTRIBUTES",
            "REASONING": "We need to convert the attributes to the correct dictionary format. For join_on_table1 and join_on_table2, we use 'unique_job_id' as it is the common key. For join_type, we use 'inner' to ensure only matching records are kept. For join_suffix, we use default. For keep_keys, we use 'both' to retain all join indices.",
            "OUTPUT": "JOIN_2([NL2SQL([[]],{'question':\"Select 'unique_job_id', 'min_experience' from 'jobs' where 'min_experience' > 1 and 'min_experience' < 3\", 'protocol':'postgres','database':'postgres', 'source':'postgres_example','collection':'public'}), NL2SQL([[]],{'question':\"Select 'unique_job_id', 'skill_required' from 'skills_required_for_job' where 'skill_required' = 'Python'\", 'protocol':'postgres','database':'postgres', 'source':'postgres_example','collection':'public'})], {'join_on_table1':'unique_job_id','join_on_table2':'unique_job_id','join_type':'inner','join_suffix':[],'keep_keys':'both'})"
            },
            {
            "TASK": "USER INTENT ALIGNMENT",
            "REASONING": "Are we answering the user's original question? Yes, we are finding jobs with a duration of more than 1 year but less than 3 years, and require Python skills. Are any additional steps needed to fully address the intent? No, the JOIN_2 operation ensures we only return jobs that meet both criteria, fully addressing the user's intent.",
            "OUTPUT": "JOIN_2([NL2SQL([[]],{'question':\"Select 'unique_job_id', 'min_experience' from 'jobs' where 'min_experience' > 1 and 'min_experience' < 3\", 'protocol':'postgres','database':'postgres', 'source':'postgres_example','collection':'public'}), NL2SQL([[]],{'question':\"Select 'unique_job_id', 'skill_required' from 'skills_required_for_job' where 'skill_required' = 'Python'\", 'protocol':'postgres','database':'postgres', 'source':'postgres_example','collection':'public'})], {'join_on_table1':'unique_job_id','join_on_table2':'unique_job_id','join_type':'inner','join_suffix':[],'keep_keys':'both'})"
            }
        ]
        

    example_tree = parse_chain(example_chain)
    # print("\nTree as dict:")
    # print(json.dumps(tree_dict, indent=2))
    
    # Mock NL_to_RUN for demonstration (simulating a synchronous function with delay)
    def mock_NL_to_RUN(tool, inp, attributes, properties):
        print(f"[{time.strftime('%H:%M:%S')}] {tool} processing with inputs and attributes {attributes}")
        # time.sleep(1)  # Simulate I/O-bound operation (e.g., database query)
        return NL_to_RUN(tool, inp, attributes, properties)  # Call the actual function for side effects/logging
        # return f"Result of {tool}"
    
    # Run the tree asynchronously
    result, output_tree = asyncio.run(example_usage(example_tree, mock_NL_to_RUN))
    print("Final result:", result)
    # print("\nOutput tree:")
    # print(json.dumps(output_tree, indent=2))





    ####EX RUN 0911 0445pm

#     jflavien@ip-10-0-165-149:~/rit-git/blue$ /bin/python /home/jflavien/rit-git/blue/demo_planners/nl_mergeplannerlinker_totree.py
# JOIN_2
# - inputs:
#   NL2SQL
#   - inputs:
#     - empty input
#   - attrs: {'question': "Select 'unique_job_id', 'min_experience' from 'jobs' where 'min_experience' > 1 and 'min_experience' < 3", 'protocol': 'postgres', 'database': 'postgres', 'collection': 'public'}
#   NL2SQL
#   - inputs:
#     - empty input
#   - attrs: {'question': "Select 'unique_job_id', 'skill_required' from 'skills_required_for_job' where 'skill_required' = 'Python'", 'protocol': 'postgres', 'database': 'postgres', 'collection': 'public'}
# - attrs: {'join_on_table1': 'unique_job_id', 'join_on_table2': 'unique_job_id', 'join_type': 'inner', 'join_suffix': [], 'keep_keys': 'both'}
# [23:44:50] Starting JOIN_2
# [23:44:50] Starting NL2SQL
# [23:44:50] Starting NL2SQL
# [23:44:50] NL2SQL processing with inputs and attributes {'question': "Select 'unique_job_id', 'min_experience' from 'jobs' where 'min_experience' > 1 and 'min_experience' < 3", 'protocol': 'postgres', 'database': 'postgres', 'collection': 'public'}
# [23:44:50] NL2SQL processing with inputs and attributes {'question': "Select 'unique_job_id', 'skill_required' from 'skills_required_for_job' where 'skill_required' = 'Python'", 'protocol': 'postgres', 'database': 'postgres', 'collection': 'public'}
# [23:44:53] Completed NL2SQL
# [23:44:54] Completed NL2SQL
# [23:44:54] JOIN_2 processing with inputs and attributes {'join_on_table1': 'unique_job_id', 'join_on_table2': 'unique_job_id', 'join_type': 'inner', 'join_suffix': [], 'keep_keys': 'both'}
# [23:44:54] Completed JOIN_2
# Final result: [[{'unique_job_id_ds0': 'job-2019-0099347:research associate, health services research and evaluation, rhs (2-yr contract):10 may 2019:company_327', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0099347:research associate, health services research and evaluation, rhs (2-yr contract):10 may 2019:company_327', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0099347:research associate, health services research and evaluation, rhs (2-yr contract):10 may 2019:company_327', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0099347:research associate, health services research and evaluation, rhs (2-yr contract):10 may 2019:company_327', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0118932:teaching partner:06 jun 2019:company_570', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0118932:teaching partner:06 jun 2019:company_570', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0118932:teaching partner:06 jun 2019:company_570', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0118932:teaching partner:06 jun 2019:company_570', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0112397:full stack developer:28 may 2019:company_215', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0112397:full stack developer:28 may 2019:company_215', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0112397:full stack developer:28 may 2019:company_215', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0112397:full stack developer:28 may 2019:company_215', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0105944:civil & structural drafter:17 may 2019:company_1438', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0105944:civil & structural drafter:17 may 2019:company_1438', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0105944:civil & structural drafter:17 may 2019:company_1438', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0105944:civil & structural drafter:17 may 2019:company_1438', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0107151:associate consultant (k2):21 may 2019:company_2145', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0107151:associate consultant (k2):21 may 2019:company_2145', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0107151:associate consultant (k2):21 may 2019:company_2145', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0107151:associate consultant (k2):21 may 2019:company_2145', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0108354:engineer, integrated operations centre:22 may 2019:company_247', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0108354:engineer, integrated operations centre:22 may 2019:company_247', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0108354:engineer, integrated operations centre:22 may 2019:company_247', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0108354:engineer, integrated operations centre:22 may 2019:company_247', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0099318:software engineer:10 may 2019:company_1110', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0099318:software engineer:10 may 2019:company_1110', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0099318:software engineer:10 may 2019:company_1110', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0099318:software engineer:10 may 2019:company_1110', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0083839:senior consultant:22 may 2019:company_763', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0083839:senior consultant:22 may 2019:company_763', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0083839:senior consultant:22 may 2019:company_763', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0083839:senior consultant:22 may 2019:company_763', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0100903:software engineer:13 may 2019:company_3632', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0100903:software engineer:13 may 2019:company_3632', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0100903:software engineer:13 may 2019:company_3632', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0100903:software engineer:13 may 2019:company_3632', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0014160:civil & structural drafter:17 may 2019:company_1438', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0014160:civil & structural drafter:17 may 2019:company_1438', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0014160:civil & structural drafter:17 may 2019:company_1438', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0014160:civil & structural drafter:17 may 2019:company_1438', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0097262:executive project engineer (m&e):08 may 2019:company_633', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0097262:executive project engineer (m&e):08 may 2019:company_633', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0097262:executive project engineer (m&e):08 may 2019:company_633', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0097262:executive project engineer (m&e):08 may 2019:company_633', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0097917:engineer:08 may 2019:company_3460', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0097917:engineer:08 may 2019:company_3460', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0097917:engineer:08 may 2019:company_3460', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0097917:engineer:08 may 2019:company_3460', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0117920:developer for sap b1:04 jun 2019:company_2207', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0117920:developer for sap b1:04 jun 2019:company_2207', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0117920:developer for sap b1:04 jun 2019:company_2207', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0117920:developer for sap b1:04 jun 2019:company_2207', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0021652:senior consultant:22 may 2019:company_763', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0021652:senior consultant:22 may 2019:company_763', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0021652:senior consultant:22 may 2019:company_763', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0021652:senior consultant:22 may 2019:company_763', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0098491:sitecore developer:09 may 2019:company_583', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0098491:sitecore developer:09 may 2019:company_583', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0098491:sitecore developer:09 may 2019:company_583', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0098491:sitecore developer:09 may 2019:company_583', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0073785:senior software engineer:14 may 2019:company_3604', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0073785:senior software engineer:14 may 2019:company_3604', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0073785:senior software engineer:14 may 2019:company_3604', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0073785:senior software engineer:14 may 2019:company_3604', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0051203:assistant research officer:29 may 2019:company_746', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0051203:assistant research officer:29 may 2019:company_746', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0051203:assistant research officer:29 may 2019:company_746', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0051203:assistant research officer:29 may 2019:company_746', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0116575:research associate:03 jun 2019:company_84', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0116575:research associate:03 jun 2019:company_84', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0116575:research associate:03 jun 2019:company_84', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0116575:research associate:03 jun 2019:company_84', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0098314:full stack software engineer:09 may 2019:company_198', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0098314:full stack software engineer:09 may 2019:company_198', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0098314:full stack software engineer:09 may 2019:company_198', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0098314:full stack software engineer:09 may 2019:company_198', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0117795:android engineer:04 jun 2019:company_234', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0117795:android engineer:04 jun 2019:company_234', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0117795:android engineer:04 jun 2019:company_234', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0117795:android engineer:04 jun 2019:company_234', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0113319:software engineer-microsoft office suite:29 may 2019:company_158', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0113319:software engineer-microsoft office suite:29 may 2019:company_158', 'skill_required': 'python'}, {'unique_job_id_ds0': 'job-2019-0113319:software engineer-microsoft office suite:29 may 2019:company_158', 'min_experience': 2.0, 'unique_job_id_ds1': 'job-2019-0113319:software engineer-microsoft office suite:29 may 2019:company_158', 'skill_required': 'python'}, ...
