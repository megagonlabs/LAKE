# Planner prompt prototypes and data descriptions used by the demo pipeline.
# Keep datasource examples generic so the repository is safe to publish.




import copy
import json
import sys
from typing import List, Dict, Any, Callable, Optional

from blue.operators.nl2llm_operator import *
from blue.operators.nl2sql_operator import *
from blue.operators.join_operator import *


from demo_planners.utils import *







#2 possibilities:

# I guess both need to have data at least from the data registry otherwise how do they know what to do? A needed info could be in dataset or should be infered from LLM.


#Other issue : what about None? Imagine there is the question infer the median salary for Company Y on job T.  But for some reason, company Y has no data on job T. The LLM could create a more complex plan where it sees how company Y position on other jobs compared to other company, and transpose this to job T based on the salary of other compamy for Job T. I guess it can be fair that first round the answer from LLM is None, and then user ask again to infer from data.


#First: the horizontal planner: only one flow of execution, no parallelism. THe planner is given every steps each time, and until it agrees that it should not be splitted again, we split


#Second: the vertical planner: The LLM directly creates a tree recursively. Each time it is called it is given the current step to split. 





# First : the horizontal planner, 
# It hsould use as entry a list of tools : as these tools can be abstract or not 
# If they are it is not executable and ened refining, else it is executable and we can run it.
# It would also make sense for the '.refine' method





initial_prompt="""You are an expert in splitting tasks into smaller subtasks from a main task. The subtasks should be among the tools available and given to you.
You're given for each tool a name and a description of what it does. You should return in JSON the sequence of tools to be used and precise instructions formulated in natural language for each tool.
Your full answer should be a list of dicts such as:
{{
'step_number': Current step number, from 1,
'reason': Justify the relevance of this step - in particular will it be able to perform the task you attribute him? Is the data available for it? Is there an intermediate step to group data or acquire it that you forgot? If it is the case, you can still change the tool and resume your reasoning from there,
'tool': Mention from the list the tool to use,
'tool_task': Mention the task to be handled by the tool
}}

{addition}
**The tool_task should be clear, precise and complete, so that the tool can be executed without further clarification. Include every relevant information that you have such as column names etc, for the subtask to be able to operate. **

**Use the information on data that you have available and are provided below to create the subtasks. If you need more information, you can probably rely on a tool to get it earlier in the plan.**

Data information: 
{data}
Here is a real example of what you should answer for the main task "{example_task}":
{example_plan}


**IT IS VERY IMPORTANT TO PAY ATTENTION TO THE TOOLS REQUIREMENTS BEFORE USING THEM**
Tools:
{available_tools}

Main task: {task}{error_mitigation}"""


###Would be good also to know what are in the cols to build the query better
#But can only be done once we know which cols we need
# So I guess this is a second kind of planner that happens when we try to execute the command
# Maybe also at this stage we would benefit from slightly deeper insights


#NOT ENOUGH
data_infos_base="""DB default_db has table job_seeker_skills 
job_seeker_skills has columns job_seeker_id, skills
DB default_db has table job_seeker_work_experiences 
job_seeker_work_experiences has columns duration_years, start_date, seqno, job_seeker_id, job_title, work_experience_text, end_date
DB default_db has table job_seeker 
job_seeker has columns job_seeker_id, education_level, skills
DB postgres has table none 
none has columns none"""

# data_infos_butnotfounderror=
data_infos_butno="""DB default_db has table job_seeker_work_experiences
Table job_seeker_work_experiences has columns:job_title (categorical - 4 examples : senior cloud developer, cloud developer, cloud application developer, senior cloud security engineer),work_experience_text (free text - 1 example: "cloud developer, finsecure technologies, singapore\njanuary 2014 - march 2018  \n- designed and implemented cloud infrastructure solutions with a focus on security and compliance for banking applications.  \n- led initiatives to enhance data protection measures, resulting in improved customer trust and satisfaction.  \n- worked closely with regulatory bodies to ensure that cloud solutions met stringent compliance requirements.**"),start_date (date),end_date (date),duration_years (double precision),seqno (bigint),job_seeker_id (bigint)
DB default_db has table job_seeker_skills
Table job_seeker_skills has columns: skills (categorical - 4 most frequent: aws, azure, docker, kubernetes), job_seeker_id (bigint)
DB default_db has table job_seeker
Table job_seeker has columns: education_level (categorical - 4 most frequent: bachelor, master), skills (free text - 1 example:".net, c#, java, sql, html, css, javascript, docker, kubernetes, aws, azure"),job_seeker_id (character varying)"""

data_infos_supposedtofix="""Tables has columns:job_title (categorical - 4 examples : senior cloud developer, cloud developer, cloud application developer, senior cloud security engineer),work_experience_text (free text - 1 example: "cloud developer, finsecure technologies, singapore\njanuary 2014 - march 2018  \n- designed and implemented cloud infrastructure solutions with a focus on security and compliance for banking applications.  \n- led initiatives to enhance data protection measures, resulting in improved customer trust and satisfaction.  \n- worked closely with regulatory bodies to ensure that cloud solutions met stringent compliance requirements.**"),start_date (date),end_date (date),duration_years (double precision),seqno (bigint),job_seeker_id (bigint)
skills (categorical - 4 most frequent: aws, azure, docker, kubernetes), job_seeker_id (bigint)
education_level (categorical - 4 most frequent: bachelor, master), skills (free text - 1 example:".net, c#, java, sql, html, css, javascript, docker, kubernetes, aws, azure"),job_seeker_id (character varying)"""

#this time data infos from the source itself so we dont make mistake
data_infos_nodetails="""
Here is the list of tables with their columns in postgres public data
           table_name             |       column_name        |     data_type     | is_nullable | column_default 
-----------------------------------+--------------------------+-------------------+-------------+----------------
 avg_min_experience_years_by_title | short_job_title          | character varying | NO          | 
 avg_min_experience_years_by_title | avg_min_experience_years | real              | YES         | 
 avg_min_salary_by_title           | short_job_title          | character varying | YES         | 
 avg_min_salary_by_title           | avg_min_salary           | double precision  | YES         | 
 benefits_for_jobs                 | unique_job_id            | character varying | YES         | 
 benefits_for_jobs                 | benefit                  | character varying | YES         | 
 categories_for_jobs               | unique_job_id            | character varying | NO          | 
 categories_for_jobs               | job_category             | character varying | YES         | 
 category_to_job_titles            | short_job_titles         | character varying | YES         | 
 category_to_job_titles            | job_cateogry             | character varying | YES         | 
 certifications_for_jobs           | unique_job_id            | character varying | YES         | 
 certifications_for_jobs           | certification            | character varying | YES         | 
 company_info                      | company_name             | character varying | YES         | 
 company_info                      | company_info             | character varying | YES         | 
 company_info                      | location_areas           | character varying | YES         | 
 company_info                      | company_id               | character varying | NO          | 
 degrees_for_jobs                  | unique_job_id            | character varying | YES         | 
 degrees_for_jobs                  | degree                   | character varying | YES         | 
 degrees_for_jobs                  | field_of_study           | character varying | YES         | 
 frequent_benefit_by_title         | short_job_title          | character varying | YES         | 
 frequent_benefit_by_title         | benefit                  | character varying | YES         | 
 frequent_benefit_by_title         | frequency                | integer           | YES         | 
 frequent_certifications_by_title  | short_job_title          | character varying | NO          | 
 frequent_certifications_by_title  | certification            | character varying | YES         | 
 frequent_certifications_by_title  | frequency                | integer           | YES         | 
 frequent_degree_by_title          | short_job_title          | character varying | YES         | 
 frequent_degree_by_title          | degree                   | character varying | YES         | 
 frequent_degree_by_title          | field_of_study           | character varying | YES         | 
 frequent_degree_by_title          | frequency                | integer           | YES         | 
 frequent_job_type_by_title        | short_job_title          | character varying | YES         | 
 frequent_job_type_by_title        | job_type                 | character varying | YES         | 
 frequent_job_type_by_title        | frequency                | integer           | YES         | 
 frequent_skill_pairs              | skill_1                  | character varying | YES         | 
 frequent_skill_pairs              | skill_2                  | character varying | YES         | 
 frequent_skill_pairs              | frequency                | integer           | YES         | 
 frequent_skills_by_title          | short_job_title          | character varying | YES         | 
 frequent_skills_by_title          | skill_required           | character varying | YES         | 
 frequent_skills_by_title          | frequency                | integer           | YES         | 
 jobs                              | company_id               | character varying | YES         | 
 jobs                              | unique_job_id            | character varying | NO          | 
 jobs                              | job_title                | character varying | YES         | 
 jobs                              | short_job_title          | character varying | YES         | 
 jobs                              | address                  | character varying | YES         | 
 jobs                              | location                 | character varying | YES         | 
 jobs                              | employment_type          | character varying | YES         | 
 jobs                              | seniority                | character varying | YES         | 
 jobs                              | min_experience           | double precision  | YES         | 
 jobs                              | posting_date             | character varying | YES         | 
 jobs                              | expiry_date              | character varying | YES         | 
 jobs                              | no_of_applications       | character varying | YES         | 
 jobs                              | min_salary               | double precision  | YES         | 
 jobs                              | max_salary               | double precision  | YES         | 
 skills_required_for_job           | unique_job_id            | character varying | YES         | 
 skills_required_for_job           | skill_required           | character varying | YES         | """


data_infos_everything="""Here is the list of tables with their columns in postgres public data
           table_name             |       column_name        |     data_type     | is_nullable | column_default 
-----------------------------------+--------------------------+-------------------+-------------+----------------
 avg_min_experience_years_by_title | short_job_title          | character varying | NO          | 
 avg_min_experience_years_by_title | avg_min_experience_years | real              | YES         | 
 avg_min_salary_by_title           | short_job_title          | character varying | YES         | 
 avg_min_salary_by_title           | avg_min_salary           | double precision  | YES         | 
 benefits_for_jobs                 | unique_job_id            | character varying | YES         | 
 benefits_for_jobs                 | benefit                  | character varying | YES         | 
 categories_for_jobs               | unique_job_id            | character varying | NO          | 
 categories_for_jobs               | job_category             | character varying | YES         | 
 category_to_job_titles            | short_job_titles         | character varying | YES         | 
 category_to_job_titles            | job_cateogry             | character varying | YES         | 
 certifications_for_jobs           | unique_job_id            | character varying | YES         | 
 certifications_for_jobs           | certification            | character varying | YES         | 
 company_info                      | company_name             | character varying | YES         | 
 company_info                      | company_info             | character varying | YES         | 
 company_info                      | location_areas           | character varying | YES         | 
 company_info                      | company_id               | character varying | NO          | 
 degrees_for_jobs                  | unique_job_id            | character varying | YES         | 
 degrees_for_jobs                  | degree                   | character varying | YES         | 
 degrees_for_jobs                  | field_of_study           | character varying | YES         | 
 frequent_benefit_by_title         | short_job_title          | character varying | YES         | 
 frequent_benefit_by_title         | benefit                  | character varying | YES         | 
 frequent_benefit_by_title         | frequency                | integer           | YES         | 
 frequent_certifications_by_title  | short_job_title          | character varying | NO          | 
 frequent_certifications_by_title  | certification            | character varying | YES         | 
 frequent_certifications_by_title  | frequency                | integer           | YES         | 
 frequent_degree_by_title          | short_job_title          | character varying | YES         | 
 frequent_degree_by_title          | degree                   | character varying | YES         | 
 frequent_degree_by_title          | field_of_study           | character varying | YES         | 
 frequent_degree_by_title          | frequency                | integer           | YES         | 
 frequent_job_type_by_title        | short_job_title          | character varying | YES         | 
 frequent_job_type_by_title        | job_type                 | character varying | YES         | 
 frequent_job_type_by_title        | frequency                | integer           | YES         | 
 frequent_skill_pairs              | skill_1                  | character varying | YES         | 
 frequent_skill_pairs              | skill_2                  | character varying | YES         | 
 frequent_skill_pairs              | frequency                | integer           | YES         | 
 frequent_skills_by_title          | short_job_title          | character varying | YES         | 
 frequent_skills_by_title          | skill_required           | character varying | YES         | 
 frequent_skills_by_title          | frequency                | integer           | YES         | 
 jobs                              | company_id               | character varying | YES         | 
 jobs                              | unique_job_id            | character varying | NO          | 
 jobs                              | job_title                | character varying | YES         | 
 jobs                              | short_job_title          | character varying | YES         | 
 jobs                              | address                  | character varying | YES         | 
 jobs                              | location                 | character varying | YES         | 
 jobs                              | employment_type          | character varying | YES         | 
 jobs                              | seniority                | character varying | YES         | 
 jobs                              | min_experience           | double precision  | YES         | 
 jobs                              | posting_date             | character varying | YES         | 
 jobs                              | expiry_date              | character varying | YES         | 
 jobs                              | no_of_applications       | character varying | YES         | 
 jobs                              | min_salary               | double precision  | YES         | 
 jobs                              | max_salary               | double precision  | YES         | 
 skills_required_for_job           | unique_job_id            | character varying | YES         | 
 skills_required_for_job           | skill_required           | character varying | YES         | 
 
 
==============================
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



example={'easy':{'task':"For job titles that have a start date that is after the biggest pic of covid cases, say if they have anything to do with COVID or not. ",
         'plan':{frozenset(['ROWWISE_NL2LLM','NL2SQL','NL2LLM']):"""[
{
'step_number': 1,
'reason': "To complete the main task, we need information about the date of the latest pic date of COVID. It looks like there is no data related to COVID from the available data, so I will take it from knowledge from LLM.",
'tool': "NL2LLM",
'tool_task': "Find the date of the biggest pic of COVID cases."
},
{
'step_number': 2,
'reason': "Now we can query the table for job titles after that day. Data structure contains column job_title and start_date, so we can use them directly",
'tool': "NL2SQL",
'tool_task': "Select the 'job_title' for elements that have a 'start_date' after the date retrieved for the biggest pic of COVID cases."
},        
{
'step_number': 3,
'reason': "We need now to state for each job title found earlier if they are related to covid. There is no such information in DB so we must query a LLM for each row.",
'tool': "ROWWISE_NL2LLM",
'tool_task': "For each 'job_title', find if they are related to COVID. Say TRUE or FALSE."
}
]""",
frozenset(['ROWWISE_NL2LLM','SMARTNL2SQL','NL2LLM']):"""[
{
'step_number': 1,
'reason': "To complete the main task, we need information about the date of the latest pic date of COVID. It looks like there is no data related to COVID from the available data, so I will take it from knowledge from LLM.",
'tool': "NL2LLM",
'tool_task': "Find the date of the biggest pic of COVID cases."
},
{
'step_number': 2,
'reason': "Now we can query the table for job titles after that day. Data structure contains column job_title and start_date, so we can use them directly",
'tool': "SMARTNL2SQL",
'tool_task': "Select the 'job_title' for elements that have a 'start_date' after the date retrieved for the biggest pic of COVID cases."
},        
{
'step_number': 3,
'reason': "We need now to state for each job title found earlier if they are related to covid. There is no such information in DB so we must query a LLM for each row.",
'tool': "ROWWISE_NL2LLM",
'tool_task': "For each 'job_title', find if they are related to COVID. Say TRUE or FALSE."
}
]"""}}}

##STEP NB TOOL TASK PREVISOULY BEGAN BY Using 'job_seeker_work_experiences' table from default_db DB, f...
additions={1:"""Your biggest issue is that you struggle to stay focused on the data available and tools purpose and usage. Often, you want to query information that is not available in the right format in the database, or try to use a tool that is not adapted because you assume previous steps that you have not completed. Mention this thinking explicitely in your reasoning for each tool to avoid this kind of issues.
           Pay attention to the data structure compared to the user question. It might be that additionnal processing through tools need to be done on database data to adapt to user question, for example to take into account different wordings or different granularities between the data and the actual question."""}

def get_plan(task,error_mitigation='',special_task={}, tools_list=['JOIN_2','SELECT','NL2LLM','ROWWISE_NL2LLM','NL2SQL']):
      addition=''
      if 'addition' in special_task.keys():
            addition=additions[special_task['addition']]


      example_task=example['easy']['task']
      for key,val in example['easy']['plan'].items():
            good=True
            for elt in key:
                  if not elt in tools_list:
                       good=False
                       break  
            if good:
                 example_plan=val
                 break
      if example_plan=='':
           raise Exception("No example plan found with the provided tools")
      prompt=initial_prompt.format(data=data_infos, addition=addition, example_task=example_task,example_plan=example_plan, available_tools=get_tool_description(tools_list, level=['basic']), task=task, error_mitigation=error_mitigation)
      return standard_NL2LLM_agent(prompt,["step_number","reason","tool","tool_task"])


def get_plan_text(plan):
    return '\n'.join(["{}.{}(\"{}\")".format(elt['step_number'],elt['tool'],elt['tool_task']) for elt in plan[0]])

