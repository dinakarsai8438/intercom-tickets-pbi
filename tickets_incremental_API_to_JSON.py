#!/usr/bin/env python
# coding: utf-8

# ## Intercom Tickets Incremental Load API to Json
# 
# 
# 

# In[9]:


Base_URL= "https://api.eu.intercom.io/"
entity_name= "tickets"
Authorization= "Bearer dG9rOmUyZDMzNGVlX2ZkNDdfNGNiZl9iNWNlXzdkMDZhZWY3Y2M4YToxOjA6ZXUtd2VzdC0x"
Intercom_Version = "2.12"


# In[10]:


import requests
import json
from pyspark.sql.functions import explode, col, expr, explode_outer,lit , udf
from pyspark.sql.types import ArrayType, StructType, MapType, IntegerType, DateType, StringType
from notebookutils import mssparkutils
from pyspark import SparkContext
from concurrent.futures import ThreadPoolExecutor


# In[11]:


# Define the path in ADLS where you want to save the files
adls_save_path = "abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/incremental_raw_json/"

# Read the conversation IDs from the ADLS folder
adls_path = "abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/incremental_ticket_ids_list/*.json"
df = spark.read.json(adls_path)


# In[12]:


display(df)


# In[13]:


# Get the list of ticket IDs
if "__ticket_id__" in df.columns:
    id_list = [row["__ticket_id__"] for row in df.select("__ticket_id__").collect()]
else:
    # If the DataFrame is empty or the 'id' column doesn't exist, set id_list to an empty list
    id_list = []

print(id_list)


# In[14]:


# Define a function to process each API request
def fetch_and_save_ticket(id):
    api_url = f"{Base_URL}{entity_name}/{id}"
    
    headers = {
        'Authorization': Authorization,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3',
        'accept': 'application/json',
        'Intercom-Version': Intercom_Version
    }
    
    # Make the API request
    response = requests.get(api_url, headers=headers)
    if response.status_code == 200:
        # Parse the JSON response
        response_data = response.json()
         # Convert the JSON response to a string
        json_data = json.dumps(response_data)
        # Define the ADLS path for each JSON response
        adls_full_path = f"{adls_save_path}{id}"
        # Write the JSON content directly to ADLS
        mssparkutils.fs.put(adls_full_path, json_data, overwrite=True)
    
    else:
        # Handle the failed request
        print(f"Failed to fetch ticket ID {id}. Status code: {response.status_code}")



# In[15]:


#threadpool executor for parllel execution
with ThreadPoolExecutor(max_workers=2) as executor:
    results = list(executor.map(fetch_and_save_ticket, id_list))

