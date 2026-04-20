from blue.operators.create_database_operator import *
from blue.operators.create_table_operator import CreateTableOperator, create_table_operator_function
from blue.operators.insert_table_operator import InsertTableOperator, insert_table_operator_function
from blue.operators.nl2llm_operator import *

from blue.operators.nl2llm_operator import *

from blue.operators.nl2sql_operator import *

from blue.operators.select_operator import *
from blue.operators.join_operator import *
from collections import defaultdict
import logging

from demo_planners.config import (
    DEFAULT_DATA_REGISTRY_NAME,
    DEFAULT_NL2SQL_COLLECTION,
    DEFAULT_NL2SQL_DATABASE,
    DEFAULT_NL2SQL_PROTOCOL,
    DEFAULT_PLATFORM_NAME,
    DEFAULT_SERVICE_URL,
    DEFAULT_SOURCE_NAME,
)
from demo_planners.nl2sql_defaults import apply_default_database_nl2sql_attributes

import re  # If not already imported outside the function

logging.basicConfig(
    level=logging.CRITICAL,  # Or DEBUG, WARNING, etc.
    format='%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s'
)


def _apply_default_blue_runtime(properties):
    """Apply publication-safe local defaults for Blue-backed demo calls."""
    properties.update(
        {
            "service_url": DEFAULT_SERVICE_URL,
            "platform.name": DEFAULT_PLATFORM_NAME,
            "data_registry.name": DEFAULT_DATA_REGISTRY_NAME,
        }
    )
    return properties


def recursive_limit_for_dico(dico, number_to_display=3):
    if type(dico)==list:
        return [recursive_limit_for_dico(x, number_to_display) for x in dico[:number_to_display]]
    elif type(dico)==dict or type(dico)==defaultdict:
        return {key:recursive_limit_for_dico(value,number_to_display) for key,value in dico.items()}
    else:
        return dico
    






data_infos="""==============================
📄 TABLE: avg_min_experience_years_by_title
==============================

🔸 Top 5 values for column: short_job_title
           short_job_title           | frequency_in_tb 
-------------------------------------+-----------------
 Manager, Global Industry Management |               2
 SAS Programmer                      |               2
 Supervisor-Docking                  |               2
 Summer Intern                       |               2
 Financial Services Consultant       |               2



🔸 Top 5 values for column: avg_min_experience_years
 avg_min_experience_years | frequency_in_tb 
--------------------------+-----------------
                        3 |            2398
                        5 |            2308
                        2 |            2304
                        1 |            2232
                        0 |            1886



==============================
📄 TABLE: avg_min_salary_by_title
==============================

🔸 Top 5 values for column: short_job_title
           short_job_title           | frequency_in_tb 
-------------------------------------+-----------------
 Manager, Global Industry Management |               2
 SAS Programmer                      |               2
 Supervisor-Docking                  |               2
 Vocals Teacher                      |               2
 Financial Services Consultant       |               2



🔸 Top 5 values for column: avg_min_salary
 avg_min_salary | frequency_in_tb 
----------------+-----------------
           5000 |            1012
           4000 |             856
           3000 |             826
           2000 |             688
           6000 |             616



==============================
📄 TABLE: benefits_for_jobs
==============================

🔸 Top 5 values for column: unique_job_id
                             unique_job_id                              | frequency_in_tb 
------------------------------------------------------------------------+-----------------
 JOB-2019-0080122:Warehouse Assistant (Grade 4):21 May 2019:company_869 |              30
 JOB-2019-0080053:Delivery Driver (Grade 3):21 May 2019:company_869     |              28
 JOB-2019-0111499:Delivery Driver (Grade 3):27 May 2019:company_869     |              28
 JOB-2019-0101241:Business Development Manager:13 May 2019:company_3020 |              26
 JOB-2019-0117248:Banquet Events Executive:04 Jun 2019:company_3020     |              24



🔸 Top 5 values for column: benefit
             benefit              | frequency_in_tb 
----------------------------------+-----------------
 5-day work week                  |             442
 attractive remuneration package  |             292
 Benefits:                        |             268
 competitive salary               |             268
 competitive remuneration package |             220



==============================
📄 TABLE: categories_for_jobs
==============================

🔸 Top 5 values for column: unique_job_id
                                                     unique_job_id                                                      | frequency_in_tb 
------------------------------------------------------------------------------------------------------------------------+-----------------
 not_available:not_available:not_available:company_63                                                                   |              20
 JOB-2019-0110740:Healthcare Partners:26 May 2019:company_14                                                            |              14
 JOB-2019-0106596:Business Development / Sales Executive:21 May 2019:company_452                                        |              14
 JOB-2019-0110947:Bespoke Travel Coordinator:27 May 2019:company_4170                                                   |              14
 JOB-2019-0099737:Supply Chain Executive (Chemical, Shipment, Documentation, Order Fulfillment):10 May 2019:company_591 |              12



🔸 Top 5 values for column: job_category
      job_category      | frequency_in_tb 
------------------------+-----------------
 Information Technology |            7166
 Engineering            |            2962
 Retail                 |            2554
 Accounting             |            2374
 Auditing               |            2374



==============================
📄 TABLE: category_to_job_titles
==============================

🔸 Top 5 values for column: short_job_titles
         short_job_titles         | frequency_in_tb 
----------------------------------+-----------------
 ['Business Development Manager'] |              22
 ['Sales Executive']              |              20
 ['Sales Engineer']               |              16
 ['Operations Executive']         |              10
 ['Recruitment Consultant']       |              10



🔸 Top 5 values for column: job_cateogry
                               job_cateogry                               | frequency_in_tb 
--------------------------------------------------------------------------+-----------------
 Banking and Finance, Consulting, Information Technology, Risk Management |               2
 Secretarial, Customer Service, Marketing                                 |               2
 Customer Service, Environment                                            |               2
 Secretarial, Risk Management                                             |               2
 Design, Engineering, Manufacturing, Others                               |               2



==============================
📄 TABLE: certifications_for_jobs
==============================

🔸 Top 5 values for column: unique_job_id
                                        unique_job_id                                        | frequency_in_tb 
---------------------------------------------------------------------------------------------+-----------------
 JOB-2019-0105147:Network & Security Specialist:17 May 2019:company_3642                     |              18
 JOB-2019-0018744:Lecturer - IT Systems & Networks:26 Jan 2019:company_639                   |              16
 JOB-2019-0101459:Server and Storage Presales Consultant  (Ref 23035):13 May 2019:company_14 |              16
 JOB-2018-0245125:ServiceNow Developer:08 May 2019:company_1709                              |              14
 JOB-2019-0109423:ServiceNow Developer:23 May 2019:company_1709                              |              14



🔸 Top 5 values for column: certification
      certification      | frequency_in_tb 
-------------------------+-----------------
 Class 3 driving license |             242
 CISSP                   |             222
 CPA                     |             200
 CCNA                    |             138
 CCNP                    |             130



==============================
📄 TABLE: company_info
==============================

🔸 Top 5 values for column: company_name
         company_name         | frequency_in_tb 
------------------------------+-----------------
 EN-WORLD SINGAPORE PTE. LTD. |               2
 GLOBALTIX PTE. LTD.          |               2
 FEHMARN CONSULTING PTE. LTD. |               2
 PERFORCE CAPITAL PTE. LTD.   |               2
 WORKDAY SINGAPORE PTE. LTD.  |               2



🔸 Top 5 values for column: company_info
                                                                                                                                                        company_info 
                                                                                                                                                       | frequency_in
_tb 
---------------------------------------------------------------------------------------------------------------------------------------------------------------------
-------------------------------------------------------------------------------------------------------------------------------------------------------+-------------
----
 No information added.                                                                                                                                               
                                                                                                                                                       |            1
432
 not_available                                                                                                                                                       
                                                                                                                                                       |             
 70
 A multidiscoplinary natural health clinic, treating pain, dysfunction and attempting to improve health.                                                             
                                                                                                                                                      +|             
  4
                                                                                                                                                                     
                                                                                                                                                       | 
 CGS-CIMB Securities International Pte. Ltd. (“CGS-CIMB”) is one of the leading integrated financial service providers in Asia. It is a 50-50 joint venture between C
hina Galaxy International Financial Holdings Limited, a wholly-owned subsidiary of China Galaxy Securities Co. Ltd., and CIMB Group Sdn Bhd.          +|             
  4
 Through a network of local offices, branches and strategic partners, we have a global presence in over 20 countries, providing a truly Asian perspective. We are wel
l-positioned as Asia’s leading financial gateway with a core focus on well researched and in-depth analysis on financial products.                    +| 
 We are a customer-centric firm and focus on value creation for clients, offering a suite of investment and financial solutions for retail and institutional clients.
 Our businesses include retail broking, institutional equities, derivatives, prime services, equities research, wealth management and online broking. +| 
 Backed by an award-winning research team, we have one of the most comprehensive research coverage of over 700 stocks in the region. Our strong research capabilities
 form the backbone of our product and service offerings, connecting clients to opportunities.                                                         +| 
                                                                                                                                                                     
                                                                                                                                                       | 
 A truly bilingual preschool provides quality education and care for children and prepare them to be responsible, respectful, competent and independent lifelong lear
ners in a safe environment that nurtures individual strengths.                                                                                        +|             
  4
                                                                                                                                                                     
                                                                                                                                                       | 



🔸 Top 5 values for column: location_areas
 location_areas | frequency_in_tb 
----------------+-----------------
 [None]         |            1590
 ['Marina']     |             804
 ['Jurong']     |             422
 [None, None]   |             392
 ['Geylang']    |             338



🔸 Top 5 values for column: company_id
  company_id  | frequency_in_tb 
--------------+-----------------
 company_3899 |               2
 company_4892 |               2
 company_1933 |               2
 company_4532 |               2
 company_4422 |               2



==============================
📄 TABLE: degrees_for_jobs
==============================

🔸 Top 5 values for column: unique_job_id
                                            unique_job_id                                            | frequency_in_tb 
-----------------------------------------------------------------------------------------------------+-----------------
 JOB-2019-0110774:Research Engineer (SCC), IHPC:26 May 2019:company_315                              |              36
 JOB-2019-0103605:Supplies Quality Customer Support Manager:15 May 2019:company_304                  |              32
 JOB-2019-0110661:APDS Dry Etch Process and Equipment Development Sr Engineer:25 May 2019:company_14 |              30
 JOB-2019-0110664:APDS Dry Etch Process and Equipment Development Sr Engineer:25 May 2019:company_14 |              30
 JOB-2019-0118919:Data Scientist:06 Jun 2019:company_2304                                            |              28



🔸 Top 5 values for column: degree
      degree      | frequency_in_tb 
------------------+-----------------
 bachelors degree |           11738
 diploma          |            6766
 degree           |            4190
 masters degree   |            3754
 bachelors        |            1096



🔸 Top 5 values for column: field_of_study
     field_of_study     | frequency_in_tb 
------------------------+-----------------
 computer science       |            3484
 engineering            |            1410
 Computer Science       |            1234
 electrical engineering |             974
 computer engineering   |             878



==============================
📄 TABLE: frequent_benefit_by_title
==============================

🔸 Top 5 values for column: short_job_title
        short_job_title        | frequency_in_tb 
-------------------------------+-----------------
 Sales Executive               |             234
 Management Trainee            |             220
 Recruitment Consultant        |             156
 Software Engineer             |             136
 Senior Recruitment Consultant |             126



🔸 Top 5 values for column: benefit
             benefit             | frequency_in_tb 
---------------------------------+-----------------
 5-day work week                 |             296
 Benefits:                       |             252
 competitive salary              |             252
 flexible hours                  |             186
 attractive remuneration package |             178



🔸 Top 5 values for column: frequency
 frequency | frequency_in_tb 
-----------+-----------------
         1 |           16674
         2 |             880
         3 |             254
         4 |             106
         5 |              70



==============================
📄 TABLE: frequent_certifications_by_title
==============================

🔸 Top 5 values for column: short_job_title
      short_job_title       | frequency_in_tb 
----------------------------+-----------------
 Project Manager            |             120
 Resident Technical Officer |              94
 Resident Engineer          |              78
 Network Engineer           |              66
 Project Engineer           |              60



🔸 Top 5 values for column: certification
      certification      | frequency_in_tb 
-------------------------+-----------------
 Class 3 driving license |             164
 CPA                     |             156
 CISSP                   |             146
 CCNA                    |             104
 CFA                     |              98



🔸 Top 5 values for column: frequency
 frequency | frequency_in_tb 
-----------+-----------------
         1 |            8116
         2 |             594
         3 |             158
         4 |              62
         8 |              18



==============================
📄 TABLE: frequent_degree_by_title
==============================

🔸 Top 5 values for column: short_job_title
  short_job_title   | frequency_in_tb 
--------------------+-----------------
 Research Fellow    |             312
 Research Assistant |             194
 Data Scientist     |             190
 Research Engineer  |             182
 Project Manager    |             182



🔸 Top 5 values for column: degree
      degree      | frequency_in_tb 
------------------+-----------------
 bachelors degree |            8840
 diploma          |            4764
 degree           |            3324
 masters degree   |            2742
 bachelors        |             944



🔸 Top 5 values for column: field_of_study
     field_of_study     | frequency_in_tb 
------------------------+-----------------
 computer science       |            2114
 engineering            |            1054
 Computer Science       |             836
 electrical engineering |             698
 a relevant field       |             680



🔸 Top 5 values for column: frequency
 frequency | frequency_in_tb 
-----------+-----------------
         1 |           22254
         2 |            2114
         3 |             574
         4 |             256
         5 |             146



==============================
📄 TABLE: frequent_job_type_by_title
==============================

🔸 Top 5 values for column: short_job_title
     short_job_title      | frequency_in_tb 
--------------------------+-----------------
 Administrative Assistant |              28
 Service Crew             |              24
 Project Manager          |              22
 Management Trainee       |              22
 Accounts Assistant       |              20



🔸 Top 5 values for column: job_type
       job_type       | frequency_in_tb 
----------------------+-----------------
 Full Time            |            7798
 Permanent            |            4682
 Permanent, Full Time |            4194
 Contract             |            1980
 Contract, Full Time  |            1232



🔸 Top 5 values for column: frequency
 frequency | frequency_in_tb 
-----------+-----------------
         1 |           16974
         2 |            2500
         3 |             844
         4 |             462
         5 |             262



==============================
📄 TABLE: frequent_skill_pairs
==============================

🔸 Top 5 values for column: skill_1
      skill_1       | frequency_in_tb 
--------------------+-----------------
 microsoft office   |            2464
 management         |            2392
 customer service   |            2090
 project management |            1968
 leadership         |            1858



🔸 Top 5 values for column: skill_2
      skill_2       | frequency_in_tb 
--------------------+-----------------
 microsoft office   |            2464
 management         |            2386
 customer service   |            2090
 project management |            1968
 leadership         |            1858



🔸 Top 5 values for column: frequency
 frequency | frequency_in_tb 
-----------+-----------------
         1 |           44648
         2 |           27252
         3 |           20568
         4 |           14736
         5 |           12800



==============================
📄 TABLE: frequent_skills_by_title
==============================

🔸 Top 5 values for column: short_job_title
  short_job_title  | frequency_in_tb 
-------------------+-----------------
 Assistant Manager |             506
 Data Scientist    |             464
 Project Manager   |             386
 Senior Consultant |             380
 Software Engineer |             360



🔸 Top 5 values for column: skill_required
   skill_required   | frequency_in_tb 
--------------------+-----------------
 Management         |           11214
 Project Management |            8684
 Leadership         |            8640
 Microsoft Office   |            7364
 Customer Service   |            6126



🔸 Top 5 values for column: frequency
 frequency | frequency_in_tb 
-----------+-----------------
         1 |          292766
         2 |           41312
         3 |           14180
         4 |            7014
         5 |            4656



==============================
📄 TABLE: jobs
==============================

🔸 Top 5 values for column: company_id
 company_id | frequency_in_tb 
------------+-----------------
 company_14 |            4290
 company_5  |             714
 company_43 |             506
 company_32 |             476
 company_0  |             472



🔸 Top 5 values for column: unique_job_id
                                                               unique_job_id                                                                | frequency_in_tb 
--------------------------------------------------------------------------------------------------------------------------------------------+-----------------
 job-2019-0100556:senior software developer:12 may 2019:company_14                                                                          |               2
 job-2019-0110154:licensed aircraft maintenance engineer:24 may 2019:company_2801                                                           |               2
 job-2019-0118582:project management office manger (lead consultant):06 jun 2019:company_567                                                |               2
 job-2019-0104470:vp, supply chain financing application lead, institutional banking group technology, t&o (wd03844):16 may 2019:company_32 |               2
 job-2019-0114710:contract staff (snr), java backend developer, ea-sre (site reliability engineering), t&o (wd04766):30 may 2019:company_32 |               2



🔸 Top 5 values for column: job_title
     job_title      | frequency_in_tb 
--------------------+-----------------
 project manager    |             286
 research fellow    |             226
 software engineer  |             224
 project engineer   |             218
 accounts executive |             202



🔸 Top 5 values for column: short_job_title
  short_job_title  | frequency_in_tb 
-------------------+-----------------
 project manager   |             450
 software engineer |             354
 project engineer  |             304
 sales executive   |             302
 research fellow   |             266



🔸 Top 5 values for column: address
                         address                         | frequency_in_tb 
---------------------------------------------------------+-----------------
 not_available                                           |            8970
 golden wall centre, 89 short street 188216              |             742
 marina bay financial centre, 12 marina boulevard 018982 |             534
 21 lower kent ridge road 119077                         |             512
 az @ paya lebar, 140 paya lebar road 409015             |             492



🔸 Top 5 values for column: location
     location      | frequency_in_tb 
-------------------+-----------------
                   |           10522
 marina            |            8982
 jurong            |            2818
 geylang           |            2566
 hong leong garden |            2492



🔸 Top 5 values for column: employment_type
   employment_type    | frequency_in_tb 
----------------------+-----------------
 full time            |           16494
 permanent            |            8706
 permanent, full time |            7218
 contract             |            3068
 contract, full time  |            2548



🔸 Top 5 values for column: seniority
    seniority     | frequency_in_tb 
------------------+-----------------
 executive        |            8432
 professional     |            5008
 manager          |            4420
 non-executive    |            4248
 senior executive |            3456



🔸 Top 5 values for column: min_experience
 min_experience | frequency_in_tb 
----------------+-----------------
              1 |            7574
              2 |            7486
              3 |            7130
              5 |            6194
              4 |            1774



🔸 Top 5 values for column: posting_date
 posting_date | frequency_in_tb 
--------------+-----------------
 28 May 2019  |            2516
 22 May 2019  |            2460
 27 May 2019  |            2434
 03 Jun 2019  |            2426
 06 Jun 2019  |            2346



🔸 Top 5 values for column: expiry_date
 expiry_date | frequency_in_tb 
-------------+-----------------
 21 Jun 2019 |            2488
 27 Jun 2019 |            2428
 26 Jun 2019 |            2332
 07 Jun 2019 |            2312
 03 Jul 2019 |            2280



🔸 Top 5 values for column: no_of_applications
 no_of_applications | frequency_in_tb 
--------------------+-----------------
 0                  |            7776
 1                  |            5150
 2                  |            3888
 3                  |            3092
 4                  |            2606



🔸 Top 5 values for column: min_salary
 min_salary | frequency_in_tb 
------------+-----------------
       3000 |            2876
       5000 |            2850
       2000 |            2726
       4000 |            2688
       2500 |            2442



🔸 Top 5 values for column: max_salary
 max_salary | frequency_in_tb 
------------+-----------------
       6000 |            2096
       5000 |            2096
       4000 |            2044
       8000 |            1906
       3000 |            1894



==============================
📄 TABLE: skills_required_for_job
==============================

🔸 Top 5 values for column: unique_job_id
                                                               unique_job_id                                                                | frequency_in_tb 
--------------------------------------------------------------------------------------------------------------------------------------------+-----------------
 recruiter:admin executive:16 may 2019:company_0                                                                                            |              80
 recruiter:management trainee (service):14 may 2019:company_3450                                                                            |              80
 job-2019-0110154:licensed aircraft maintenance engineer:24 may 2019:company_2801                                                           |              40
 job-2019-0104470:vp, supply chain financing application lead, institutional banking group technology, t&o (wd03844):16 may 2019:company_32 |              40
 job-2019-0118582:project management office manger (lead consultant):06 jun 2019:company_567                                                |              40



🔸 Top 5 values for column: skill_required
   skill_required   | frequency_in_tb 
--------------------+-----------------
 management         |           25828
 project management |           19248
 leadership         |           18902
 microsoft office   |           16692
 customer service   |           14248"""

#To replace with data discovery



# #**An NL2SQL cannot take an input, it uses data from database** I addded this for NL2SQL, something that we would like the auto prompt or genetic refiner to do
operators_description = {'NL2SQL':{'basic':"""Convert a natural language question into a SQL query to run on a database. You cannot ingest elements from previous steps.
Returns the rows from the table matching the query, including its columns (fields) names. Be sure that the columns you are using belong to a single table.
! DO NOT: Use NL2SQL to operate on data retrieved on a previous step.
! Bad reasoning example : I will use data from the earlier step XX to obtain our result. Correct behavior : Use another tool.""",'advanced':"""""",'linking':"""
attributes:
-question: what we want to obtain from the query
-protocol: default "%s"; supported values include ["postgres", "mysql"]
-source: use the configured datasource name, currently "%s"
-database: default "%s"
-collection: default "%s"
-context : can be empty, used to provide additionnal details
Returns the rows from the table matching the query, including its columns (fields) names.""" % (
    DEFAULT_NL2SQL_PROTOCOL,
    DEFAULT_SOURCE_NAME,
    DEFAULT_NL2SQL_DATABASE,
    DEFAULT_NL2SQL_COLLECTION,
)},
                         'ROWWISE_NL2LLM':{'basic':"""Given a natural language query, fetch open-domain knowledge from LLM.
Returns the input table augmented with new columns attr_names containing LLM result of question attribute on each row.""",'advanced':"""""",
                       'linking':"""attributes:
-query:what we want to obtain from the query
-context: can be empty, used to provide additionnal details 
-attr_names: a list of string, each string being keys you want in the output
Returns the input table augmented with new columns attr_names containing LLM result of question attribute on each row."""},
                         'NL2LLM':{'basic':"""Given a natural language query, fetch open-domain knowledge from LLM using the input as context.
Returns in output the answer to the question's attribute, placed inside attr_names field(s), relying on common knowledge and on input if provided.
**This should be used to get general knowledge, don't use it for instance to perform a calculation on data from a previous step**""",'advanced':"""""",
'linking':"""attributes:
-query:what we want to obtain from the query
-attr_names: a list of string, each string being keys you want in the output (must not be empty)
Returns in output the answer to the question's attribute, placed inside attr_names field(s), relying on common knowledge and on input if provided."""},
# 'SMARTNL2SQL':{'basic':"""Convert a natural language question into a SQL query to run on a database, or on the inputs.
#     If you want to run on a database, provide empty inputs [[]] - still providing the attributes - the database will be queried.
#     Returns the rows from the table matching the query, including its columns (fields) names. 
#     """,'advanced':"""""" ,'linking':"""attributes:
#     -question: what we want to obtain from the query. If processing an input, the table name you provide if needed should ONLY be 'tb', as it is how the input will be inserted in the SQLite db. The input list can only contain one item. Use JOIN if you want to process multiple inputs.
#     -context : can be empty, used to provide additionnal details
#     Returns the rows from the table matching the query, including its columns (fields) names."""},

 'SMARTNL2SQL': {
  'basic': """Convert a natural language question into a SQL query to run on a database, or on the inputs.
  If you want to run on a database, provide empty inputs [[]].
  If you want to process input data, the only available table name is 'tb'.
  Returns the rows from the table matching the query, including its columns (fields) names.
  """,
  'advanced': "",
  'linking': """attributes:
  - question: the natural language query to convert.
  - runOn : must be either 'database' or 'input'
  - context: optional context.
  ⚠️ IMPORTANT:
  - When inputs are provided, the table name MUST be 'tb'. Do NOT use any other table name like 'jobs', 'users', etc. 
  - When inputs are empty ([[]]), the database will be queried instead.
  - The input list can only contain one item.
  
  Returns: rows from the matching table, including column names.
  """
},
'COUNT':{'basic':"""Count the number of elements in the first element of input, input being a list of list.""",'advanced':"""""",
'linking':"""
attributes:
Always provide \{\} as attributes, the empty dictonary
Returns [[{'count':NB_ELEMENTS}]]"""},
                         'MULTIPLY':{'basic':"""Multiply each input number by a given factor.
Returns the input multiplied by the factor.""",'advanced':""""""},
#                          'JOIN':{'basic':"""Given an input data,  comprising two or more lists of data, return n-way join. Remember that every input element should be in the same input as a list of list of dictionary. **Use APPEND operator to merge outputs from two steps into a single one**
# **You CANNOT perform a join if the elements you want to join have not yet been outputted by a previous step, and have not been appended in a single output with APPEND**
# **You CANNOT join from a table that is still in the database and haven't been pulled by one of the step, and furthemore appended to a unique output**
# Returns the joined data based on the specified join conditions and attributes.""",'advanced':""""""},
'JOIN':{'basic':"""Given an input data,  comprising two or more lists of data, return n-way join. To use outputs from different steps, use APPEND operator to merge outputs.
**The description cannot mention several steps : if you need to do that then it means that you are missing an APPEND step before**
**You CANNOT perform a join if the elements you want to join have not yet been outputted by a previous step, and have not been appended in a single output with APPEND**
**You CANNOT join from a table that is still in the database and haven't been pulled by one of the step**
Returns the joined data based on the specified join conditions and attributes.""",'advanced':""""""},
'JOIN_2':{'basic':"""Perform n-way join on outputs from two different steps.
**Both input to join have to be already outputted by a previous step, JOIN_2 cannot use directly data from database : It requires two earlier steps that have for goal to retrieve the data to join.**
Returns the joined data based on the specified join conditions and attributes.
! Bad usage: Using JOIN_2 on data that was not retrieved by 2 previous steps.
! Bad reasoning example: I will join the result from last step with the table from database. Correct behavior : Use another tool to retrieve data first before using JOIN_2.""",'advanced':"""""",
                       'linking':"""
This tool requires two inputs, that you can give in a list: JOIN_2(INPUT1,INPUT2,ATTRIBUTES)
attributes:
-join_on_table1: the column name to join on in the first table.
-join_on_table2: the column name to join on in the second table. **Should be from a field compatible with join_on_table1.**
-join_type: type str, type of join in 'inner', 'left', 'right', 'outer'
-join_suffix: type list of str, each element (str) refers to the additional suffix to add to a data source/group field names to avoid conflicts. The default suffix is ‘_ds{i}’ for data source/group i in the input data.
-keep_keys: type str, one value in ‘left’, ‘both’. 
--‘left’: keep only the join index of the first data group in the input data
--‘both’: keep all join indices with suffixes.
Returns the joined data based on the specified join conditions and attributes."""},
                         'SELECT':{'basic':"""Given an input data, consisting of a list of data elements,  filter data elements based on a specified condition (record-wise). The select will operate on input data provided in output from previous steps.
**You can only perform basic operations with this operator : max, min, or the operators <,>,=,=<,=> compared with a specific unique value to provide. You CANNOT compare or match on a list of elements : JOIN should be used for that. The column/key on which we perform the operation needs to be known also**
**This tool can ONLY be used on already retrieved data: it should be in the output of one of the previous step**
Returns the input data with only the records that match the specified condition.""",'advanced':"""""",
                       'linking':"""attributes:
operand_key: type str, the key to filter the data records. Should be the exact key
operand: type str, comparison operator: =, !=, >, >=, <, <=, max, min, in, not in, like, not like
operand_val: type Any, value to compare with (not needed for max, min)
approximate_match: type bool, whether to use epsilon tolerance for numeric comparison.
eps: type float, epsilon tolerance for numeric comparison
Returns the input data with only the records that match the specified condition."""},
#                          'APPEND':{'basic':"""Given an input data, consisting of a list of data elements, append a new data element to the input data. Useful to merge results from different steps for certain tools.
# Returns the input data with the new element appended.""",'advanced':""""""},}
'APPEND':{'basic':"""Merge the input data with the input data given as new element attribute. Useful to merge results from different steps for certain tools.
It can simply concat the output from two steps, but not perform more complex operations. 
Returns the input data with the new element appended.""",'advanced':""""""},}







operators_description_merge = {'NL2SQL':{'basic':"""Convert a natural language question into a SQL query to run on a database. 
Returns the rows from the table matching the query, including its columns (fields) names. Be sure that the columns you are using belong to a single table.""",'advanced':"""""",'linking':"""
attributes:
-question: what we want to obtain from the query
-protocol: default "%s"; supported values include ["postgres", "mysql"]
-source: use the configured datasource name, currently "%s"
-database: default "%s"
-collection: default "%s"
-context : can be empty, used to provide additionnal details
Returns the rows from the table matching the query, including its columns (fields) names.""" % (
    DEFAULT_NL2SQL_PROTOCOL,
    DEFAULT_SOURCE_NAME,
    DEFAULT_NL2SQL_DATABASE,
    DEFAULT_NL2SQL_COLLECTION,
)},
# 'SMARTNL2SQL':{'basic':"""Convert a natural language question into a SQL query to run on a database, or on the inputs.
#     If you want to run on a database, provide empty list as inputs [] - still providing the attributes - the database will be queried.
#     Returns the rows from the table matching the query, including its columns (fields) names. 
#     """,'advanced':"""""" ,'linking':"""attributes:
#     -question: what we want to obtain from the query. If processing an input, the table name you provide if needed should ONLY be 'tb', as it is how the input will be inserted in the SQLite db. The input list can only contain one item. Use JOIN if you want to process multiple inputs.
#     -context : can be empty, used to provide additionnal details
#     Returns the rows from the table matching the query, including its columns (fields) names."""},

'SMARTNL2SQL': {
  'basic': """Convert a natural language question into a SQL query to run on a database, or on the inputs.
  If you want to run on a database, provide empty inputs [[]].
  If you want to process input data, the only available table name is 'tb'.
  Returns the rows from the table matching the query, including its columns (fields) names.
  """,
  'advanced': "",
  'linking': """attributes:
  - question: the natural language query to convert.
  - runOn : must be either 'database' or 'input'
  - context: optional context.
  ⚠️ IMPORTANT:
  - When inputs are provided, the table name MUST be 'tb'. Do NOT use any other table name like 'jobs', 'users', etc.
  - When inputs are empty ([[]]), the database will be queried instead.
  - The input list can only contain one item.
  
  Returns: rows from the matching table, including column names.
  """
},
'COUNT':{'basic':"""Count the number of elements in the first element of input, input being a list of list""",'advanced':"""""",'linking':"""
attributes:
Always provide \{\} as attributes, the empty dictonary
Returns [[{'count':NB_ELEMENTS}]]"""},
                         'ROWWISE_NL2LLM':{'basic':"""Given a natural language query, fetch open-domain knowledge from LLM.
Returns the input table augmented with new columns attr_names containing LLM result of question attribute on each row.""",'advanced':"""""",
                       'linking':"""attributes:
-query:what we want to obtain from the query
-context: can be empty, used to provide additionnal details 
-attr_names: a list of string, each string being keys you want in the output
Returns the input table augmented with new columns attr_names containing LLM result of question attribute on each row."""},
                         'NL2LLM':{'basic':"""Given a natural language query, fetch open-domain knowledge from LLM using the input as context.
Returns in output the answer to the question's attribute, placed inside attr_names field(s), relying on common knowledge and on input if provided.
**This should be used to get general knowledge, don't use it for instance to perform a calculation on data**""",'advanced':"""""",
'linking':"""attributes:
-query:what we want to obtain from the query
-attr_names: a list of string, each string being keys you want in the output (must not be empty)
Returns in output the answer to the question's attribute, placed inside attr_names field(s), relying on common knowledge and on input if provided."""},

                         'MULTIPLY':{'basic':"""Multiply each input number by a given factor.
Returns the input multiplied by the factor.""",'advanced':""""""},
'JOIN':{'basic':"""Given an input data,  comprising two or more lists of data, return n-way join. To use outputs from different steps, use APPEND operator to merge outputs.
**The description cannot mention several steps : if you need to do that then it means that you are missing an APPEND step before**
**You CANNOT perform a join if the elements you want to join have not yet been outputted by a previous step, and have not been appended in a single output with APPEND**
**You CANNOT join from a table that is still in the database and haven't been pulled by one of the step**
Returns the joined data based on the specified join conditions and attributes.""",'advanced':""""""},
'JOIN_2':{'basic':"""Perform n-way join on two inputs.
Returns the joined data based on the specified join conditions and attributes.
**You CANNOT just mention a table name as input, it needs to be retrieved. If you simply need a whole table you can use NL2SQL**""",'advanced':"""""",
                       'linking':"""
This tool requires two inputs, that you can give in a list: JOIN_2(INPUT1,INPUT2,ATTRIBUTES)
attributes:
-join_on_table1: the column name to join on in the first table.
-join_on_table2: the column name to join on in the second table. **Should be from a field compatible with join_on_table1.**
-join_type: type str, type of join in 'inner', 'left', 'right', 'outer'
-join_suffix: type list of str, each element (str) refers to the additional suffix to add to a data source/group field names to avoid conflicts. The default suffix is ‘_ds{i}’ for data source/group i in the input data.
-keep_keys: type str, one value in ‘left’, ‘both’. 
--‘left’: keep only the join index of the first data group in the input data
--‘both’: keep all join indices with suffixes.
Returns the joined data based on the specified join conditions and attributes."""},
                         'SELECT':{'basic':"""Given an input data, consisting of a list of data elements,  filter data elements based on a specified condition (record-wise). The select will operate on input data.
**You can only perform basic operations with this operator : max, min, or the operators <,>,=,=<,=> compared with a specific unique value to provide. You CANNOT compare or match on a list of elements : JOIN should be used for that. The column/key on which we perform the operation needs to be known also**
Returns the input data with only the records that match the specified condition.""",'advanced':"""""",
                       'linking':"""attributes:
operand_key: type str, the key to filter the data records. Should be the exact key
operand: type str, comparison operator: =, !=, >, >=, <, <=, max, min, in, not in, like, not like
operand_val: type Any, value to compare with (not needed for max, min)
approximate_match: type bool, whether to use epsilon tolerance for numeric comparison.
eps: type float, epsilon tolerance for numeric comparison
Returns the input data with only the records that match the specified condition."""},
'APPEND':{'basic':"""Merge the input data with the input data given as new element attribute. Useful to merge results from different steps for certain tools.
It can simply concat the output from two steps, but not perform more complex operations. 
Returns the input data with the new element appended.""",'advanced':""""""},}













operators_frequent_errors_plan_level={'JOIN':{'errors':
                                              [
                                                  {'error':"",
                                                    'source':"human",
                                                    'nb_times_useful':0,
                                                    'nb_times_displayed':0}
                                              ],
                                              'number_times_problematic':0,
                                              'total calls':0},
                                        'NL2SQL':{'errors':
                                              [
                                                  {'error':"",
                                                    'source':"human",
                                                    'nb_times_useful':0,
                                                    'nb_times_displayed':0}
                                              ],
                                              'number_times_problematic':0,
                                              'total calls':0},
}


tools_base_descriptions={'NL2SQL':{'basic':"""Convert a natural language question into a SQL query to run on a database.
Returns the rows from the table matching the query, including its columns (fields) names.""",'advanced':""""""},
# 'SMARTNL2SQL':{'basic':"""Convert a natural language question into a SQL query to run on a database, or on the inputs.
#     If you want to run on a database, provide empty inputs [[]] - still providing the attributes - the database will be queried.
#     Returns the rows from the table matching the query, including its columns (fields) names. 
#     """,'advanced':"""""" ,'linking':"""attributes:
#     -question: what we want to obtain from the query. If processing an input, the table name you provide if needed should ONLY be 'tb', as it is how the input will be inserted in the SQLite db. The input list can only contain one item. Use JOIN if you want to process multiple inputs.
#     -context : can be empty, used to provide additionnal details
#     Returns the rows from the table matching the query, including its columns (fields) names."""},
'SMARTNL2SQL': {
  'basic': """Convert a natural language question into a SQL query to run on a database, or on the inputs.
  If you want to run on a database, provide empty inputs [[]].
  If you want to process input data, the only available table name is 'tb'.
  Returns the rows from the table matching the query, including its columns (fields) names.
  """,
  'advanced': "",
  'linking': """attributes:
  - question: the natural language query to convert.
  - runOn : must be either 'database' or 'input'
  - context: optional context.
  ⚠️ IMPORTANT:
  - When inputs are provided, the table name MUST be 'tb'. Do NOT use any other table name like 'jobs', 'users', etc.
  - When inputs are empty ([[]]), the database will be queried instead.
  - The input list can only contain one item.
  
  Returns: rows from the matching table, including column names.
  """
},
'ROWWISE_NL2LLM':{'basic':"""Given a natural language query, fetch open-domain knowledge from LLM.
Returns the input table augmented with new columns attr_names containing LLM result of question attribute on each row.""",'advanced':""""""},
'NL2LLM':{'basic':"""Given a natural language query, fetch open-domain knowledge from LLM using the input as context.
Returns in output the answer to the question's attribute, placed inside attr_names field(s), relying on common knowledge and on input if provided.""",'advanced':""""""},
'MULTIPLY':{'basic':"""""",'advanced':""""""},
'COUNT':{'basic':"""Count the number of elements in the first element of input, input being a list of list
Returns [[{'count':NB_ELEMENTS}]]""",'advanced':""""""},
'JOIN':{'basic':"""Given an input data,  comprising two or more lists of data, return n-way join.
Returns the joined data based on the specified join conditions and attributes.""",'advanced':""""""},
'JOIN_2':{'basic':"""""",'advanced':""""""},
'SELECT':{'basic':"""""",'advanced':""""""},
'APPEND':{'basic':"""""",'advanced':""""""},}

tools_rules={'NL2SQL':{'basic':"""1. This tool cannot operate on results from previous steps. It can only be used to query the database.
                       2. Do not hallucinate column names.
                       3. Ensure the columns you're using belong to a single table. Otherwise, you may need multiple NL2SQL and a JOIN_2.""",'advanced':"""""",
                       'linking':"""
attributes:
-question: what we want to obtain from the query
-protocol: one of ["postgres", "mysql"]
-source: one of ["default"]
-database: choose between postgres or any other
-collection: choose between public or any other collection
-context : can be empty, used to provide additionnal details
Returns the rows from the table matching the query, including its columns (fields) names."""},
# 'SMARTNL2SQL':{'basic':"""""",'advanced':"""""" ,'linking':"""attributes:
#     -question: what we want to obtain from the query. If processing an input, the table name you provide if needed should ONLY be 'tb', as it is how the input will be inserted in the SQLite db. The input list can only contain one item. Use JOIN if you want to process multiple inputs.
#     -context : can be empty, used to provide additionnal details
#     Returns the rows from the table matching the query, including its columns (fields) names."""},
'SMARTNL2SQL': {
  'basic': """Convert a natural language question into a SQL query to run on a database, or on the inputs.
  If you want to run on a database, provide empty inputs [[]].
  If you want to process input data, the only available table name is 'tb'.
  Returns the rows from the table matching the query, including its columns (fields) names.
  """,
  'advanced': "",
  'linking': """attributes:
  - question: the natural language query to convert.
  - runOn : must be either 'database' or 'input'
  - context: optional context.
  ⚠️ IMPORTANT:
  - When inputs are provided, the table name MUST be 'tb'. Do NOT use any other table name like 'jobs', 'users', etc.
  - When inputs are empty ([[]]), the database will be queried instead.
  - The input list can only contain one item.
  
  Returns: rows from the matching table, including column names.
  """
},
'ROWWISE_NL2LLM':{'basic':"""""",'advanced':"""""",
                       'linking':"""attributes:
-query:what we want to obtain from the query
-context: can be empty, used to provide additionnal details 
-attr_names: a list of string, each string being keys you want in the output
Returns the input table augmented with new columns attr_names containing LLM result of question attribute on each row."""},
'NL2LLM':{'basic':"""1. This tool serves to get general knowledge, don't use it on other tasks such as performing a calculation on data from a previous step""",'advanced':"""""",
                       'linking':"""attributes:
-query:what we want to obtain from the query
-attr_names: a list of string, each string being keys you want in the output (must not be empty)
Returns in output the answer to the question's attribute, placed inside attr_names field(s), relying on common knowledge and on input if provided."""},
'MULTIPLY':{'basic':"""""",'advanced':"""""",
                       'linking':""""""},
'COUNT':{'basic':"""Count the number of elements in the first element of input, input being a list of list
Returns [[{'count':NB_ELEMENTS}]]""", 'linking':"""
attributes:
Always provide \{\} as attributes, the empty dictonary"""},
'JOIN':{'basic':"""1. Do not operate on outputs from multiple steps: they need to be merged first.""",'advanced':"""""",
                       'linking':""""""},
'JOIN_2':{'basic':"""1. Join must be performed on the output from 2 previous steps.
2. Do NOT try to use more than 2 steps at a time. If needed, use JOIN_2 more than once.""",'advanced':"""""",
                       'linking':"""
attributes:
-join_on_table1: the column name to join on in the first table.
-join_on_table2: the column name to join on in the second table. **Should be from a field compatible with join_on_table1.**
-join_type: type str, type of join in 'inner', 'left', 'right', 'outer'
-join_suffix: type list of str, each element (str) refers to the additional suffix to add to a data source/group field names to avoid conflicts. The default suffix is ‘_ds{i}’ for data source/group i in the input data.
-keep_keys: type str, one value in ‘left’, ‘both’. 
--‘left’: keep only the join index of the first data group in the input data
--‘both’: keep all join indices with suffixes.
Returns the joined data based on the specified join conditions and attributes."""},
'SELECT':{'basic':"""1. Do not execute other tasks than max, min, comparisons - it cannot do SQL queries.
          2. This tool only operate on data that has been retrieved by an earlier step""",'advanced':"""""",
                       'linking':"""attributes:
operand_key: type str, the key to filter the data records. Should be the exact key
operand: type str, comparison operator: =, !=, >, >=, <, <=, max, min, in, not in, like, not like
operand_val: type Any, value to compare with (not needed for max, min)
approximate_match: type bool, whether to use epsilon tolerance for numeric comparison.
eps: type float, epsilon tolerance for numeric comparison
Returns the input data with only the records that match the specified condition."""},
'APPEND':{'basic':"""""",'advanced':"""""",
                       'linking':""""""},}

tools_examples={'NL2SQL':{'basic':"""Select XX from YY where ZZ includes ...""",'advanced':""""""},
'ROWWISE_NL2LLM':{'basic':"""""",'advanced':""""""},
'NL2LLM':{'basic':"""Find the most popular language of 2022 and put it in a field 'most_popular_language_2022'""",'advanced':""""""},
'MULTIPLY':{'basic':"""""",'advanced':""""""},
'JOIN':{'basic':"""""",'advanced':""""""},
'JOIN_2':{'basic':"""Join the output from step XX and step YY on column ZZ""",'advanced':""""""},
'SELECT':{'basic':"""Select the max for XX in output of step YY""",'advanced':""""""},
'APPEND':{'basic':"""Append output from step YY to output of step XX""",'advanced':""""""},}





import time

def call_with_retry(fct,*args, max_retries=15, delay=2, **kwargs):
    for attempt in range(max_retries):
        try:
            return fct(*args, **kwargs)
        except TimeoutError as e:
            logging.critical(f"TimeoutError: {e}. Retrying {attempt+1}/{max_retries}...")
            time.sleep(delay)
        except Exception as e:
            logging.critical(f"Exception: {e}. Args:{args}. Retrying {attempt+1}/{max_retries}...")
    raise TimeoutError("Max retries exceeded for nl2llm_operator_function")

def standard_NL2LLM_agent(question, attributes_output):
    input_data = [[]]
    nl2llm_operator = NL2LLMOperator()
    properties_NL2LLM = nl2llm_operator.properties
    properties_NL2LLM['service_url'] = DEFAULT_SERVICE_URL
    attributes_NL2LLM = {
        "query": question,
        "context": "",
        "attrs": [{"name": name} for name in (attributes_output or [])],
    }
    output=call_with_retry(nl2llm_operator_function,input_data, attributes_NL2LLM, properties_NL2LLM)
    return output


def get_count(inp, attributes, properties):
    """
    Count rows within nested tool output structures.

    Many operators return data shaped as [[{...record...}, ...]] or
    more deeply nested lists. This helper walks the structure and counts
    the number of dictionary rows (falling back to scalar items when no
    dictionaries are present).
    """

    def _count_items(value):
        if isinstance(value, list):
            if value and all(isinstance(x, dict) for x in value):
                return len(value)
            return sum(_count_items(item) for item in value)
        if isinstance(value, dict):
            return 1
        return 1 if value is not None else 0

    if not inp:
        total = 0
    else:
        total = _count_items(inp[0])

    return [[{'count': total}]]

def get_standard_NL2LLM_agent(inp, attributes, properties):
    input_data = inp
    nl2llm_operator = NL2LLMOperator()
    properties= nl2llm_operator.properties
    properties['service_url'] = DEFAULT_SERVICE_URL
    if isinstance(attributes, dict) and 'attrs' not in attributes and 'attr_names' in attributes:
        names = attributes.get('attr_names') or []
        if isinstance(names, list):
            attributes = dict(attributes)
            attributes['attrs'] = [{"name": str(name)} for name in names]
    return call_with_retry(nl2llm_operator_function,input_data, attributes, properties)

# def standard_NL2LLM_agent(question, attributes_output):
#     input_data = [[]]
#     nl2llm_operator = NL2LLMOperator()
#     properties_NL2LLM = nl2llm_operator.properties

#     properties_NL2LLM['service_url'] = 'ws://localhost:8001'  # update this to your service url
#     attributes_NL2LLM = {
#         "query": question,
#         "context": "",
#         # "attr_names": ["language", "popularity_rank", "description"],
#         "attr_names": attributes_output
#     }
#     output=nl2llm_operator_function(input_data, attributes_NL2LLM, properties_NL2LLM)
#     return output



def iterate(input_to_iter, function_to_use, attributes, properties, lambda_where_to_apply_input):
    # attr=copy.deepcopy(attributes)
    res=[]
    for ielt,inp in enumerate(input_to_iter[0]):
        inp,attr,prop = lambda_where_to_apply_input(inp, attributes, properties)
        result = function_to_use([inp], attr, prop)
        if ielt>500:
            logging.critical("Rowwise NL2LLM operator: More than 500 rows, stopping iteration to avoid excessive calls to LLM")
            break
        # print(f"=== ITERATE TMP RESULT ===")
        # print(result)
        # print('input')
        # print(inp)
        # The NL2LLM operator returns a JSON array. Treat the first object as the
        # rowwise annotation and gracefully handle empty outputs.
        annotation = {}
        try:
            if isinstance(result, list) and result and isinstance(result[0], list) and result[0]:
                if isinstance(result[0][0], dict):
                    annotation = result[0][0]
            elif isinstance(result, list) and result and isinstance(result[0], dict):
                annotation = result[0]
        except Exception:
            annotation = {}

        if isinstance(inp, dict):
            res += [annotation | inp]
        else:
            # Unexpected shape; keep the input as-is to avoid crashing the whole run.
            res += [inp]
    return [res]

def wrapped_nl2llm(inp,attr,prop):
    # Planner output in this repo often uses `attr_names` instead of the operator's
    # `attrs` field. Map it so the NL2LLM operator can enforce output fields.
    if isinstance(attr, dict) and 'attrs' not in attr and 'attr_names' in attr:
        names = attr.get('attr_names') or []
        if isinstance(names, list):
            attr = dict(attr)
            attr.pop('attr_names', None)
            attr['attrs'] = [{"name": str(name)} for name in names]
    return call_with_retry(nl2llm_operator_function,inp,attr,prop)

def rowwise_nl2llm_operator_function(input_data, attributes_NL2LLM, properties_NL2LLM):
    nl2llmoperator = NL2LLMOperator()
    properties = nl2llmoperator.properties
    def set_attr(input_data, attributes, properties):
        attributes['context'] = input_data
        return input_data, attributes, properties
    iterate_result = iterate(input_data, wrapped_nl2llm, attributes_NL2LLM, properties, set_attr)
    return iterate_result





def get_custom_NL2SQL_agent(inp,attributes,properties):
    
    """
    In this version, if there is an input, we use it to perform NL2SQL on it

    Convert a natural language question into a SQL query to run on a database, or on the inputs.
    If you want to run on a database, provide empty inputs [[]] - still providing the attributes - the database will be queried.
    Returns the rows from the table matching the query, including its columns (fields) names. 
    attributes:
    -question: what we want to obtain from the query. If processing an input, the table name you provide if needed should ONLY be 'tb', as it is how the input will be inserted in the SQLite db. The input list can only contain one item. Use JOIN if you want to process multiple inputs.
    -context : can be empty, used to provide additionnal details
    Returns the rows from the table matching the query, including its columns (fields) names.
    """
    # try:


    if inp is None:
        logging.critical('MAGICNL2SQL: inp is None, setting to [[]]')
        inp=[[]]
    if not type(inp)==list:
        raise Exception('MAGICNL2SQL: inp is not list')
    if len(inp)==0:
        inp=[[]]
    if inp[0] is None:  
        logging.critical('MAGICNL2SQL: inp[0] is None, setting to [[]]')
        inp=[[]]
    # except Exception as e:
    #     logging.critical(f'MAGICNL2SQL: inp is invalid, setting to [[]]. Error: {e}. Input: {inp}')
    #     inp=[[]]
    input_data = inp
    nl2sql_operator = NL2SQLOperator()
    properties = nl2sql_operator.properties
    # logging.critical('DEBUG attributes:'+json.dumps(attributes))
    _apply_default_blue_runtime(properties)
    properties['service_url'] = DEFAULT_SERVICE_URL
    
    if attributes['runOn']=='database':#inp==[[]] or inp==[] or len(inp)==0 or len(inp[0])==0:
        logging.critical('MAGICNL2SQL: NL2SQL on database')
        attributes = apply_default_database_nl2sql_attributes(attributes)
        return call_with_retry(nl2sql_operator_function,input_data, attributes, properties)
    elif attributes['runOn']=='input':

        logging.critical('MAGICNL2SQL: NL2SQL on inputs')
        attributes_createdb = {
            "source":  "internal",  
            "database": "tmp1",
            "overwrite": True,
        }


        # Get default properties
        createdb_operator = CreateDatabaseOperator()
        properties_createdb = createdb_operator.properties
        _apply_default_blue_runtime(properties_createdb)

        # logging.critical('1')
        # call the function
        # Option 1: directly call the nl2sql_operator_function
        result = create_database_operator_function([[]], attributes_createdb, properties_createdb)
        if not type(inp[0])==list:
            logging.critical('MAGICNL2SQL: inp has not enough dim, going to 2')
            logging.critical("Input is:"+json.dumps(inp))
            # logging.critical('MAGICNL2SQL: inp shape:'+str(len(inp))+'x'+str(len(inp[0]))+'x'+str(len(inp[0][0])))
            # logging.critical('MAGICNL2SQL: inp :'+json.dumps(inp))
            inp=[inp]
        if type(inp[0][0])==list:
            logging.critical('MAGICNL2SQL: inp has too many dim, going to 2')
            logging.critical("Input is:"+json.dumps(inp))
            # logging.critical('MAGICNL2SQL: inp shape:'+str(len(inp))+'x'+str(len(inp[0]))+'x'+str(len(inp[0][0]))+'x'+str(len(inp[0][0][0])))
            # logging.critical('MAGICNL2SQL: inp :'+json.dumps(inp))
            inp=inp[0]
        # logging.critical('2')
        if inp==[[]] or len(inp)==0 or len(inp[0])==0:
            logging.critical('MAGICNL2SQL: inp is empty and running on inputs, returning [[]]')
            return [[]]
        try:
            columns=[{'name': key} for key in inp[0][0].keys()]
        except Exception as e:
            logging.critical("traceback is:"+traceback.format_exc())
            logging.critical("Input is:"+json.dumps(inp))
            raise e
        attributes_createtable = {
            "source": "internal",#sqlite_test_source",
            "database": "tmp1",
            "collection": "public",
            "table": "tb",
            "description": "",
            "created_by": "",
            "properties": {"version": "0.1"},
            "columns": columns,
            # "misc": {
            #     "primary_key": ["skill_id"],
            #     "foreign_keys": [{"foreign_keys_source_columns": ["resume_id"], "foreign_keys_target_table": "resume", "foreign_keys_target_columns": ["resume_id"]}],
            # },
            "overwrite": True,
        }

        # logging.critical('3')
        # Get default properties
        createtable_operator = CreateTableOperator()
        properties_createtable = createtable_operator.properties
        _apply_default_blue_runtime(properties_createtable)


        # call the function
        # Option 1: directly call the nl2sql_operator_function
        result = create_table_operator_function([[]], attributes_createtable, properties_createtable)
        
        # logging.critical('4')

        # Example attributes
        attributes_inserttable = {"source": "internal", "database": "tmp1", "collection": "public", "table": "tb", "batch_size": 100}

        # Example properties
        properties_inserttable = {"platform.name": DEFAULT_PLATFORM_NAME, "data_registry.name": DEFAULT_DATA_REGISTRY_NAME}

        # Get default properties
        inserttable_operator = InsertTableOperator()
        properties_inserttable = inserttable_operator.properties
        _apply_default_blue_runtime(properties_inserttable)

        # logging.critical('5')
        # call th   e function
        # Option 1: directly call the nl2sql_operator_function
        result = insert_table_operator_function(input_data, attributes_inserttable, properties_inserttable)
        # logging.critical('6')
        attributes['source']='internal'
        attributes['protocol']='sqlite'
        attributes['database']='tmp1'
        attributes['collection']='public'
        attributes['question']= re.sub(r'FROM\s+\w+', 'FROM tb', attributes['question'], flags=re.I)

        return call_with_retry(nl2sql_operator_function,[[]], attributes, properties)
    else:
        raise Exception("MAGICNL2SQL: runOn attribute must be either 'database' or 'input'")
    


def get_standard_NL2SQL_agent(inp,attributes,properties):
    input_data = inp
    nl2sql_operator = NL2SQLOperator()
    properties = nl2sql_operator.properties
    # logging.critical('DEBUG attributes:'+json.dumps(attributes))
    _apply_default_blue_runtime(properties)
    properties['service_url'] = DEFAULT_SERVICE_URL
    attributes = apply_default_database_nl2sql_attributes(attributes)
    return call_with_retry(nl2sql_operator_function,input_data, attributes, properties)


def get_standard_join_operator(inp,attributes,properties):
    return join_operator_function(inp,attributes)
    

def lowercase_dict_values(obj):
    if isinstance(obj, dict):
        return {
            k: lowercase_dict_values(v) if not isinstance(v, str) else v.lower()
            for k, v in obj.items()
        }
    elif isinstance(obj, list):
        return [lowercase_dict_values(item) for item in obj]
    elif isinstance(obj, tuple):
        return tuple(lowercase_dict_values(item) for item in obj)
    else:
        return obj

def get_standard_join_2_operator(inp,attributes,properties):
    if 'new_element' not in attributes.keys():
        attributes['join_on']=[[attributes['join_on_table1']],[attributes['join_on_table2']]]
        return join_operator_function(lowercase_dict_values(inp[0]+inp[1]),attributes)
    else:
        new_elt=attributes['new_element']
        attributes['join_on']=[[attributes['join_on_table1']],[attributes['join_on_table2']]]
        if type(new_elt)==list and type(new_elt[0])==list:
            inp= inp+new_elt
        elif type(new_elt)==list:
            logging.critical("Append operator: linker missed one dimension for input. Putting appended element in the correct format.")
            inp= inp+[new_elt]
        return join_operator_function(lowercase_dict_values(inp),attributes)
    


def get_append_operator(inp,attributes,properties):
    new_elt=attributes['new_element']
    if type(new_elt)==list and type(new_elt[0])==list:
        return inp+new_elt
    elif type(new_elt)==list:
        logging.critical("Append operator: linker missed one dimension for input. Putting appended element in the correct format.")
        return inp+[new_elt]
    else:
        raise Exception('New element to merge should be a list')

def get_standard_select_operator(inp,attributes,properties):
    # logging.critical('Select operator debug: Input: '+str(input))
    # logging.critical('Select operator debug: attributes: '+str(attributes))
    # logging.critical('Select operator debug: properties: '+str(properties))
    operand = attributes.get('operand')
    operand_key = attributes.get('operand_key')
    if 'operand_val' in list(attributes.keys()) and type(attributes['operand_val'])==str and attributes['operand_val'].lower() in['true','false'] and inp and type(inp)==list and inp[0][0] and type(inp[0][0])==dict and operand_key in inp[0][0].keys() and type(inp[0][0][operand_key])==bool :
        attributes['operand_val']= True if attributes['operand_val'].lower()=='true' else False
    
    if operand in ['like', 'not like']:
        data = inp[0] if inp and isinstance(inp[0], list) else []
        if not data or not operand_key:
            return [[]]
        if 'operand_val' not in attributes:
            raise Exception("Select requires an operand_val key in attributes for 'like' or 'not like'")
        operand_val = attributes['operand_val']
        if not isinstance(operand_val, str):
            raise Exception("operand_val must be a string pattern for 'like' or 'not like'")
        
        # Convert SQL-like pattern to regex (case-insensitive)
        pattern = '^' + re.escape(operand_val).replace('\\%', '.*').replace('\\_', '.') + '$'
        regex = re.compile(pattern, re.IGNORECASE)
        
        if operand == 'like':
            result = [rec for rec in data if regex.match(str(rec.get(operand_key, '')))]
        else:  # 'not like'
            result = [rec for rec in data if not regex.match(str(rec.get(operand_key, '')))]
        return [result]
    if operand in ['max', 'min']:
        data = inp[0] if inp and isinstance(inp[0], list) else []
        if not data or not operand_key or not all(operand_key in rec for rec in data):
            return [[]]
        # Find the max/min value for the key
        values = [rec[operand_key] for rec in data if type(rec.get(operand_key)) in [int,float,str]]
        target_value = max(values) if operand == 'max' else min(values)
        # Return all records with that value (in case of ties)
        result = [[rec for rec in data if rec[operand_key] == target_value][0]]
        return [result]
    elif operand in ['in', 'not in']:
        data = inp[0] if inp and isinstance(inp[0], list) else []
        if not data or not operand_key:
            return [[]]
        # Ensure operand_val is a list for 'in'/'not in'
        if not 'operand_val' in attributes.keys():
            raise Exception("Select requests a operand_val key in attributes if the operand is not in the list ['max','min','in','not in','like','not like']")
        operand_val= attributes.get('operand_val')
        if not isinstance(operand_val, list):
            operand_val = [operand_val]
        if operand == 'in':
            result = [rec for rec in data if rec.get(operand_key) in operand_val]
        else:  # 'not in'
            result = [rec for rec in data if rec.get(operand_key) not in operand_val]
        return [result]
    else:
        if not 'operand_val' in attributes.keys():
            raise Exception("Select requests a operand_val key in attributes if the operand is not in the list ['max','min','in','not in','like','not like']")
        return select_operator_function(inp, attributes, properties)



###TOOLS DESCRIPTION

def get_available_tools():
    return list(operators_description.keys())

def get_tool_description(items, level=['basic'], version='base', type='base'):
    """get the text for a list of tools, with basic level or more advanced. The more advanced could come with examples or be dynamic"""
    res_txt=""

    if version=='with_rules':
        for elt in tools_base_descriptions.keys():
            if items=='*' or elt in items:
                res_txt+=elt+'\n'+'Description: '+'\n'.join([tools_base_descriptions[elt][x] for x in level if x in tools_base_descriptions[elt].keys()])+'\n'
                res_txt+='Rules: '+'\n'.join([tools_rules[elt][x] for x in level if x in tools_rules[elt].keys()])+'\n'
                res_txt+='Example of task: '+'\n'.join([tools_examples[elt][x] for x in level if x in tools_examples[elt].keys()])+'\n\n'
        return res_txt[:-2]
    elif version=='base':
        if type=='themergeone':
            ope=operators_description_merge
        else:
            ope=operators_description
        for elt in operators_description:
            if items=='*' or elt in items:
                res_txt+=elt+'\n'+'\n'.join([ope[elt][x] for x in level])+'\n\n'
        return res_txt[:-2]


import re

def get_nested_value(data, index_str):
    # Example: index_str = "input[2][1][0]"
    # Extract all numbers inside brackets
    indexes = [int(i) for i in re.findall(r'\[(\d+)\]', index_str)]
    # Remove the prefix (e.g., "input")
    value = data
    for idx in indexes:
        value = value[idx]
    return value


def set_nested_value(input_tmp, content, destination_str):
    """
    Sets `content` at the position defined by destination_str (e.g., '[2][1][0]') in input_tmp.
    Expands lists as needed.
    Returns the modified input_tmp.
    """
    import re
    indexes = [int(i) for i in re.findall(r'\[(\d+)\]', destination_str)]
    if not indexes:
        return content  # If no index, just replace

    # Work on a copy to avoid side effects if needed
    ref = input_tmp
    for i, idx in enumerate(indexes):
        # If we're at the last index, set the value
        if i == len(indexes) - 1:
            # Expand list if needed
            while len(ref) <= idx:
                ref.append([])
            ref[idx] = content
        else:
            # Expand list if needed
            while len(ref) <= idx:
                ref.append([])
            # If next level is not a list, make it a list
            if not isinstance(ref[idx], list):
                ref[idx] = []
            ref = ref[idx]
    return input_tmp



# a = [[]]
# a = set_nested_value(a, "foo", "[0]")
# a = set_nested_value(a, "bar", "[1]")
# a = set_nested_value(a, "lo", "[1]")
# a = set_nested_value(a, "se", "[0]")
# a = set_nested_value(a, "vv", "[3]")
# print(a)  # Output: ['foo', 'bar']

def dictlist_to_markdown(data, max_rows=10):
    """
    Convert nested list(s) of dicts into a Markdown table.
    Automatically right-aligns numeric columns.

    Args:
        data (list): Possibly nested list of dictionaries.
        max_rows (int): Number of rows to display before truncating.

    Returns:
        str: Markdown table string.
    """
    # Flatten one level if needed
    if data is None:
        return "No results"
    flat = [item for sublist in data for item in sublist] if any(isinstance(x, list) for x in data) else data
    if not flat:
        return "_No data_"

    # Collect all possible keys
    keys = sorted({k for d in flat for k in d.keys()})

    # Detect numeric columns
    def is_numeric(val):
        try:
            float(val)
            return True
        except (TypeError, ValueError):
            return False

    numeric_keys = set()
    for k in keys:
        if any(is_numeric(d.get(k)) for d in flat):
            numeric_keys.add(k)

    # Header
    header = "| " + " | ".join(keys) + " |\n"

    # Separator: right-align numeric columns
    separator = "| " + " | ".join(["--:" if k in numeric_keys else ":--" for k in keys]) + " |\n"

    # Rows
    rows = []
    for d in flat[:max_rows]:
        row = "| " + " | ".join(str(d.get(k, "")) for k in keys) + " |"
        rows.append(row)

    # Combine everything
    md = header + separator + "\n".join(rows)

    # Add note if truncated
    remaining = len(flat) - max_rows
    if remaining > 0:
        md += f"\n\n(+ {remaining} more rows)"

    return md

from openai import OpenAI


def get_answer_gpt(system, user):
    return get_answer_gpt_advanced(system, [user])

def get_answer_gpt_advanced(system, list_messages,model_name="gpt-5"):
    client = OpenAI()  # or rely on environment variable

    messages=[
            {"role": "system", "content": system}
        ]

    for ielt,elt in enumerate(list_messages):
        if ielt%2==1:
            messages+=[{'role':'assistant','content':elt}]
        else:
            messages+=[{'role':'user','content':elt}]
        
    with client.chat.completions.stream(
        model=model_name,
        messages=messages
    ) as stream:
        full_response = ""
        for event in stream:
            # Only handle text delta events
            # print('event',event,'\n')
            # print(event.type)
            if event.type == "content.delta":
                text_chunk = event.delta
                if text_chunk:
                    # print(text_chunk, end="", flush=True)
                    full_response += text_chunk

    return full_response








def build_nested_plan(raw_plan, ancestor_dico, operators_linking):
    # Convert raw_plan from list of dicts to step lookup
    step_lookup = {step["step_number"]: step for step in raw_plan[0]}

    def get_attributes(step_num):
        """Extract attributes from operators_linking"""
        attrs = {}
        for link in operators_linking.get(str(step_num), []):
            key = link["INPUT_KEY"].replace("attributes->", "")
            if "->" not in link["INPUT_KEY"] and "attributes->" not in link["INPUT_KEY"]:
                continue
            src = link["INPUT_SOURCE"].strip("#$")
            if key and not key.startswith("STEP") and key not in ("input", "new_element"):
                attrs[key] = src
        return attrs

    def build_step(step_num):
        """Recursively build nested structure"""
        step = step_lookup[step_num]
        ancestors = ancestor_dico.get(str(step_num), [])
        inputs = []

        for a in ancestors:
            if a == 0:  # skip START
                continue
            inputs.append(build_step(a))

        node = {
            "tool": step["tool"],
            "inputs": inputs,
        }

        attrs = get_attributes(step_num)
        if attrs:
            node["attributes"] = attrs

        return node

    # find final step (the one not an ancestor of any)
    all_steps = set(step_lookup.keys()) - {0}
    all_ancestors = set(a for v in ancestor_dico.values() for a in v if a != 0)
    final_steps = list(all_steps - all_ancestors)

    # build plan tree (handle multiple final outputs if any)
    plan_tree = [build_step(f) for f in final_steps]
    return plan_tree[0] if len(plan_tree) == 1 else plan_tree
