#!/usr/bin/env python
# coding: utf-8

# ## incremental API to Json
# 
# 
# 

# In[1]:


Base_URL= "https://api.eu.intercom.io/"
entity_name= "conversations"
Authorization= "Bearer dG9rOmUyZDMzNGVlX2ZkNDdfNGNiZl9iNWNlXzdkMDZhZWY3Y2M4YToxOjA6ZXUtd2VzdC0x"
Intercom_Version = "2.11"


# In[2]:


import requests
import json
from pyspark.sql.functions import explode, col, expr, explode_outer,lit , udf
from pyspark.sql.types import ArrayType, StructType, MapType, IntegerType, DateType, StringType
from notebookutils import mssparkutils
from pyspark import SparkContext
from concurrent.futures import ThreadPoolExecutor


# In[3]:


# Define the path in ADLS where you want to save the files
adls_save_path = "abfss://intercom-conversation-history@nourishinternal.dfs.core.windows.net/incremental_raw_json/"

# Read the conversation IDs from the ADLS folder
adls_path = "abfss://intercom-conversation-history@nourishinternal.dfs.core.windows.net/incremental_conversation_ids_list/*.json"
df = spark.read.json(adls_path)


# In[4]:


if 'id' in df.columns:
    id_list = [row['id'] for row in df.select("id").collect()]
else:
    # If the DataFrame is empty or the 'id' column doesn't exist, set id_list to an empty list
    id_list = []


# In[12]:


# Define the path in ADLS where JSON files are stored
adls_raw_json_path = "abfss://intercom-conversation-history@nourishinternal.dfs.core.windows.net/incremental_raw_json/"

id_list = [str(id) for id in id_list]

# Step 1: Get the file names (IDs) from the raw_json folder
file_status_list = mssparkutils.fs.ls(adls_save_path)

# Extract the IDs from the file names (removing the .json extension)
existing_id_list = [file.name for file in file_status_list]

# Step 2: Subtract the processed IDs from the original ID list
final_id_list = list(set(id_list) - set(existing_id_list))

#print(len(final_id_list))
#print(len(existing_id_list))
#print(len(id_list))


# In[6]:


# Define a function to process each API request
def fetch_and_save_conversation(id):
    api_url = f"{Base_URL}{entity_name}/{id}?display_as=string"
    
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
        print(f"Failed to fetch conversation ID {id}. Status code: {response.status_code}")


# In[7]:


with ThreadPoolExecutor(max_workers=10) as executor:
    results = list(executor.map(fetch_and_save_conversation, final_id_list))

