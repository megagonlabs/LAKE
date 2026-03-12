

from blue.operators.nl2llm_operator import *



###USEFUL FOR :

# rowwise_nl2llm_operator_function(input_data, attributes_NL2LLM, properties_NL2LLM)

# WORKS JUST LIKE nl2llm_operator_function

def iterate(input_to_iter, function_to_use, input_data, attributes, properties, lambda_where_to_apply_input):
    # attr=copy.deepcopy(attributes)
    res=[]
    for inp in input_to_iter:
        inp,attr,prop = lambda_where_to_apply_input(inp, attributes, properties)
        result = function_to_use([inp], attr, prop)
        # print(f"=== ITERATE TMP RESULT ===")
        # print(result)
        res+= [result[0][0]|inp]
    return res



def rowwise_nl2llm_operator_function(input_data, attributes_NL2LLM, properties_NL2LLM):
    def set_attr(input_data, attributes, properties):
        attributes['context'] = input_data
        return input_data, attributes, properties
    iterate_result = iterate(result, nl2llm_operator_function, input_data, attributes_NL2LLM, properties_NL2LLM, set_attr)
    return iterate_result


if __name__ == "__main__":
    from blue.operators.nl2sql_operator import *
    input_data = [[]]
    attributes = {

        # "question": "Find all unique job title, limit to 20 total job title.",  #ADD THE CONSTRAINT OF 20 FOR TESTING
        "question": "find durations in postgres, limit to 10 results",
        "protocol": "postgres",
        "database": "postgres",
        "collection": "public",
        "case_insensitive": True,
        "additional_requirements": "",
        "context": "",#This is a job database with information about job postings, skills, companies, and salaries",
        # schema will be fetched automatically if not provided
    }


    # just used to get the default properties
    nl2sql_operator = NL2SQLOperator()
    properties = nl2sql_operator.properties
    properties['service_url'] = 'ws://localhost:8001'  # update this to your service url

    result = nl2sql_operator_function(input_data, attributes, properties)


    print("=== NL2SQL RESULT ===")
    print(result)

    nl2llm_operator = NL2LLMOperator()
    properties_NL2LLM = nl2llm_operator.properties

    properties_NL2LLM['service_url'] = 'ws://localhost:8001'  # update this to your service url
    attributes_NL2LLM = {
        "query": "This job title requires proficiency in any programming languages? True or False?",
        "context": "",
        "attr_names": ["proficiency"]
    }




    iterate_result=rowwise_nl2llm_operator_function(result, attributes_NL2LLM, properties_NL2LLM)
    print("=== ITERATE RESULT ===")
    print(iterate_result)