#!/usr/bin/env python
# coding: utf-8

# # Setup - Install Libraries

# In[1]:


# Run the following commands once, in order to install libraries - DO NOT Uncomment this line.

# Uncomment below lines

#!pip3 install --upgrade pip
#!pip3 install google-cloud-bigquery
#!pip3 install pandas-gbq -U
#!pip3 install db-dtypes
#!pip3 install packaging --upgrade


# In[2]:


get_ipython().system('pip3 install --upgrade pip')


# In[3]:


get_ipython().system('pip3 install google-cloud-bigquery')


# In[4]:


get_ipython().system('pip3 install pandas-gbq -U')


# In[5]:


get_ipython().system('pip3 install db-dtypes')


# In[6]:


get_ipython().system('pip3 install packaging --upgrade')


# In[7]:


import sys
sys.executable


# In[8]:


import pandas_gbq
print(pandas_gbq.__version__)


# # Import libraries

# In[9]:


# Import libraries
from google.cloud import bigquery
import pandas as pd
from pandas_gbq import to_gbq
import os

print('Libraries imported successfully')


# In[10]:


# Set the environment variable for Google Cloud credentials
# Place the path in which the .json file is located.

# Example (if .json is located in the same directory with the notebook)
# os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "at-arch-416714-6f9900ec7.json"

# -- YOUR CODE GOES BELOW THIS LINE
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "C:/Users/kotas/Documents/Corporate Data Analytics (course)/Capstone/data-analytics-corporate-ka-5fc0c27e0e73.json" # Edit path
# -- YOUR CODE GOES ABOVE THIS LINE


# In[11]:


# Set your Google Cloud project ID and BigQuery dataset details

# -- YOUR CODE GOES BELOW THIS

project_id = 'data-analytics-corporate-ka' # Edit with your project id
dataset_id = 'reporting_db' # Modify the necessary schema name: staging_db, reporting_db etc.
table_id = 'rep_revenue_per_period_test' # Modify the necessary table name: stg_customer, stg_city etc.

# -- YOUR CODE GOES ABOVE THIS LINE


# # SQL Query

# In[12]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# -- YOUR CODE GOES BELOW THIS LINE

# Define your SQL query here
query = """

WITH payments AS (
  SELECT *
  FROM `data-analytics-corporate-ka.staging_db.stg_payment`
),

rentals AS (
  SELECT *
  FROM `data-analytics-corporate-ka.staging_db.stg_rental`
),

inventory AS (
  SELECT *
  FROM `data-analytics-corporate-ka.staging_db.stg_inventory`
),

films AS (
  SELECT *
  FROM `data-analytics-corporate-ka.staging_db.stg_film`
),

-- Filter valid payments excluding GOODFELLAS SALUTE
valid_payments AS (
  SELECT
    p.payment_amount,
    p.payment_date
  FROM payments p
  JOIN rentals r ON p.payment_rental_id = r.rental_id
  JOIN inventory i ON r.rental_inventory_id = i.inventory_id
  JOIN films f ON i.inventory_film_id = f.film_id
  WHERE f.film_title != 'GOODFELLAS SALUTE'
),

-- Revenue aggregates
revenue_by_period AS (
  SELECT 'Day' AS reporting_period, DATE_TRUNC(payment_date, DAY) AS reporting_date, SUM(payment_amount) AS total_revenue
  FROM valid_payments
  GROUP BY 1, 2

  UNION ALL

  SELECT 'Month', DATE_TRUNC(payment_date, MONTH), SUM(payment_amount)
  FROM valid_payments
  GROUP BY 1, 2

  UNION ALL

  SELECT 'Year', DATE_TRUNC(payment_date, YEAR), SUM(payment_amount)
  FROM valid_payments
  GROUP BY 1, 2
),

-- Join with full period calendar to include all from 2015
final AS (
  SELECT
    rpt.reporting_period,
    rpt.reporting_date,
    COALESCE(rbp.total_revenue, 0) AS total_revenue
  FROM `data-analytics-corporate-ka.reporting_db.reporting_periods_table` rpt
  LEFT JOIN revenue_by_period rbp
    ON rpt.reporting_period = rbp.reporting_period
   AND rpt.reporting_date = rbp.reporting_date
  WHERE rpt.reporting_date >= '2015-01-01'
)

-- Final insert
SELECT
  reporting_period,
  reporting_date,
  total_revenue
FROM final;
"""

# -- YOUR CODE GOES ABOVE THIS LINE

# Execute the query and store the result in a dataframe
df = client.query(query).to_dataframe()

# Explore some records
df.head()


# # Write to BigQuery

# In[13]:


# Define the full table ID
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# -- YOUR CODE GOES BELOW THIS LINE
# Define table schema based on the project description

schema = [
    bigquery.SchemaField('total_revenue', 'INTEGER'),
    bigquery.SchemaField('reporting_period', 'STRING'),
    bigquery.SchemaField('reporting_date', 'DATETIME'),
    ]

# -- YOUR CODE GOES ABOVE THIS LINE


# In[14]:


# Create a BigQuery client
client = bigquery.Client(project=project_id)

# Check if the table exists
def table_exists(client, full_table_id):
    try:
        client.get_table(full_table_id)
        return True
    except Exception:
        return False

# Write the dataframe to the table (overwrite if it exists, create if it doesn't)
if table_exists(client, full_table_id):
    # If the table exists, overwrite it
    destination_table = f"{dataset_id}.{table_id}"
    # Write the dataframe to the table (overwrite if it exists)
    to_gbq(df, destination_table, project_id=project_id, if_exists='replace')
    print(f"Table {full_table_id} exists. Overwritten.")
else:
    # If the table does not exist, create it
    job_config = bigquery.LoadJobConfig(schema=schema)
    job = client.load_table_from_dataframe(df, full_table_id, job_config=job_config)
    job.result()  # Wait for the job to complete
    print(f"Table {full_table_id} did not exist. Created and data loaded.")


# In[16]:


# Below line converts your i.pynb file to .py python executable file. Modify the input and output names based
# on the table you are processing.
# Example:
# ! jupyter nbconvert stg_customer.ipynb --to python

# -- YOUR CODE GOES BELOW THIS LINE

get_ipython().system('python3 -m jupyter nbconvert stg_address.ipynb --to python')

# -- YOUR CODE GOES ABOVE THIS LINE


# In[32]:


get_ipython().system('python3 -m pip install nbconvert')


# In[1]:


get_ipython().system('python3 -m pip install nbconvert -U')


# In[ ]:




