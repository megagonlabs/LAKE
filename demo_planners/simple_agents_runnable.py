#This file has objective to deal of NL to runnable
#We get tools, metaplan, etc...
#But we need to be able to run the actual tools

#This file also is very bad in the sense that it is not very flexible, and very hardcoded. But good enough to test things

#so first how to run stuffs
#we need a simple dico where NL stuffs are linked to the actual tools

from demo_planners.utils import *

dico_NL_to_Tool={'NL2LLM':get_standard_NL2LLM_agent, 'COUNT':get_count, 'ROWWISE_NL2LLM':rowwise_nl2llm_operator_function, 'NL2SQL':get_standard_NL2SQL_agent, 'SMARTNL2SQL':get_custom_NL2SQL_agent, 'SELECT':get_standard_select_operator, 'JOIN':get_standard_join_operator ,'JOIN_2':get_standard_join_2_operator , 'APPEND':get_append_operator}


#then we need base functions to run it


def NL_to_RUN(NLTool, inp, attributes, properties):
    # The tool-tree runner passes child outputs as a list-of-inputs. Most tools here
    # expect a single "table-ish" input shaped like List[List[Dict]], not wrapped
    # as [List[List[Dict]]]. Normalize that common case.
    if inp is None:
        inp = [[]]
    if isinstance(inp, list) and len(inp) == 1 and NLTool not in {"JOIN", "JOIN_2", "APPEND"}:
        inp = inp[0]

    output = dico_NL_to_Tool[NLTool](inp, attributes, properties)
    return output



