from blue.operators.nl2llm_operator import *

def iterate(input_to_iter, function_to_use, input_data, attributes, properties, lambda_where_to_apply_input):
    res=[]
    for inp in input_to_iter:
        inp,attr,prop = lambda_where_to_apply_input(inp, attributes, properties)
        result = function_to_use([inp], attr, prop)
        res+= [result[0][0]|inp]
    return res



def rowwise_nl2llm_operator_function(input_data, attributes_NL2LLM, properties_NL2LLM):
    def set_attr(input_data, attributes, properties):
        attributes['context'] = input_data
        return input_data, attributes, properties
    iterate_result = iterate(result, nl2llm_operator_function, input_data, attributes_NL2LLM, properties_NL2LLM, set_attr)
    return iterate_result
