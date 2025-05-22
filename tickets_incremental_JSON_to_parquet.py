#!/usr/bin/env python
# coding: utf-8

# ## Intercom Tickets Incremental Load Json To Parquet
# 
# 
# 

# In[1]:


from pyspark.sql.functions import explode, col, expr, explode_outer,lit
from pyspark.sql.types import ArrayType, StructType, MapType, IntegerType, DateType, StructField, StringType, DoubleType
import json
from notebookutils import mssparkutils


# In[3]:


adls_path =f"abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/incremental_raw_json/"
#df = spark.read.option("multiline", "true").json(adls_path)
# Check if there are any files in the incremental_raw_json folder
file_paths = mssparkutils.fs.ls(adls_path)
#print(len(file_paths))


# In[ ]:


# If files are found, read the JSON; if not, skip
if len(file_paths) > 0:
    df = spark.read.option("multiline", "true").json(adls_path)
else:
    print("No JSON files found in the incremental_raw_json folder. Skipping processing.")


# In[5]:


#display(df)


# In[6]:


def find_parent_array_fields(schema):
    array_fields = []
    for field in schema.fields:
        #field_name = f"{parent_name}.{field.name}" if parent_name else field.name
        #print(field.dataType)
        if isinstance(field.dataType, ArrayType):
             array_fields.append(field.name)
        elif isinstance(field.dataType, (StructType, MapType)):
            nested_array_fields = find_parent_array_fields(field.dataType)
            if nested_array_fields:
                array_fields.append(field.name)
    return array_fields


# In[7]:


array_parents = find_parent_array_fields(df.schema)
for i in array_parents:
    print(i)


# In[8]:


def find_non_array_parent_fields(schema):
    non_array_fields = []
    parent_array_fields = find_parent_array_fields(schema)
    def add_fields(schema):
        for field in schema.fields:
            if field.name not in parent_array_fields:
                non_array_fields.append(field.name)
    add_fields(schema)
    return non_array_fields


# In[9]:


rest_of_fields= find_non_array_parent_fields(df.schema)


# In[10]:


for i in rest_of_fields:
    print(i)


# In[11]:


def flatten(df):
    complex_fields = dict([(field.name, field.dataType) 
                           for field in df.schema.fields 
                           if isinstance(field.dataType, (ArrayType, StructType, MapType))])
    
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        #print(f"Processing: {col_name} Type: {type(complex_fields[col_name])}")

        if isinstance(complex_fields[col_name], StructType):
            expanded = [col(f"{col_name}.{k}").alias(f"{col_name}_{k}") 
                        for k in [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)

        elif isinstance(complex_fields[col_name], ArrayType):
            df = df.withColumn(col_name, explode_outer(col(col_name)))

        elif isinstance(complex_fields[col_name], MapType):
            # Explode the map into key-value pairs
            df = df.selectExpr("*", f"explode({col_name}) as ({col_name}_key, {col_name}_value)")
            # Flatten the map key and value
            df = df.withColumn(f"{col_name}_key", col(f"{col_name}_key"))
            df = df.withColumn(f"{col_name}_value", col(f"{col_name}_value"))
            df = df.drop(col_name)

        complex_fields = dict([(field.name, field.dataType) 
                               for field in df.schema.fields 
                               if isinstance(field.dataType, (ArrayType, StructType, MapType))])
    
    return df


# In[13]:


df_flattened=flatten(df)
display(df_flattened)


# In[11]:


add_column_ddl = []

# First part: Flatten the rest_of_fields
selected_df_1 = df.select(*rest_of_fields)
flattened_df_1 = flatten(selected_df_1)

# Rename columns by replacing spaces, colons, and question marks, and adding double underscores
for column in flattened_df_1.columns:
    if ' ' in column or ':' in column or '?' in column or '-' in column or '(' in column or ')' in column or '/' in column:
        flattened_df_1 = flattened_df_1.withColumnRenamed(
            column, 
            f"__{column.replace(' ', '').replace(':', '').replace('?', '').replace('-', '').replace('(', '').replace(')','').replace('/','')}__")
    else:
        flattened_df_1 = flattened_df_1.withColumnRenamed(column, f"__{column}__")

# Write parquet to ADLS
output_path = (
    f"abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/"
    f"incremental_parquet/intercom_tickets_rest_of_fields/"
)
flattened_df_1.write.mode("overwrite").parquet(output_path)

# Generate schema with column types
schema_1 = flattened_df_1.schema
columns_with_types_1 = []

for field in schema_1.fields:
    if isinstance(field.dataType, IntegerType):
        columns_with_types_1.append({"name": field.name, "type": "INT"})
    elif isinstance(field.dataType, DateType):
        columns_with_types_1.append({"name": field.name, "type": "DATETIME"})
    elif isinstance(field.dataType, StringType):
        columns_with_types_1.append({"name": field.name, "type": "NVARCHAR(MAX)"})
    elif isinstance(field.dataType, DoubleType):
        columns_with_types_1.append({"name": field.name, "type": "FLOAT"})
    else:
        columns_with_types_1.append({"name": field.name, "type": "NVARCHAR(MAX)"})

# Read original schema from ADLS and compare for differences
input_path_1 = (
    "abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/"
    "schema/intercom_tickets_schema/intercom_tickets_rest_of_fields/*.json"
)
df_rest_of_fields_schema = spark.read.option("multiline", "true").json(input_path_1)
json_str = df_rest_of_fields_schema.select("columns").collect()[0]["columns"]
original_columns_list_1 = json.loads(json_str)

# Convert lists to sets for comparison
columns_with_types_1_set = {(item['name'], item['type']) for item in columns_with_types_1}
original_columns_list_1_set = {(item['name'], item['type']) for item in original_columns_list_1}

# Calculate the difference
difference_1 = columns_with_types_1_set - original_columns_list_1_set

# Convert the difference back to a list of dictionaries
difference_list_1 = [{'name': name, 'type': type_} for name, type_ in difference_1]

if difference_list_1:
    new_columns_1 = [f"{col['name']} {col['type']}" for col in difference_list_1]

    # Generate ALTER TABLE statement without parentheses
    add_column_ddl_query_1 = f"ALTER TABLE intercom_tickets_rest_of_fields ADD {', '.join(new_columns_1)};"
    add_column_ddl.append(add_column_ddl_query_1)

    # Replace old schema with new schema
    final_columns_1 = original_columns_list_1 + difference_list_1
    output_path_1 = (
        f"abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/"
        f"schema/intercom_tickets_schema/intercom_tickets_rest_of_fields/"
    )

    # Convert final_columns_1 to JSON string
    new_schema_json_1 = json.dumps(final_columns_1)

    # Create a DataFrame with the JSON string (wrapped in a list to match DataFrame format)
    df_new_schema_1 = spark.createDataFrame([(new_schema_json_1,)], ["columns"])

    # Coalesce the DataFrame to a single partition
    df_new_schema_single_partition_1 = df_new_schema_1.coalesce(1)

    # Save the JSON string to ADLS
    df_new_schema_single_partition_1.write.mode("overwrite").json(output_path_1)

# Only the ALTER TABLE part is generated here
for array_field in array_parents:
    selected_df = df.select("id", array_field)
    flattened_df = flatten(selected_df)

    for column in flattened_df.columns:
        if ' ' in column or ':' in column or '?' in column or '-' in column or '(' in column or ')' in column or '/' in column:
            flattened_df = flattened_df.withColumnRenamed(
                column, 
                f"__{column.replace(' ', '').replace(':', '').replace('?', '').replace('-', '').replace('(', '').replace(')','').replace('/','')}__"
            )
        else:
            flattened_df = flattened_df.withColumnRenamed(column, f"__{column}__")

    # Write array field parquet to ADLS
    output_path_array = (
        f"abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/"
        f"incremental_parquet/intercom_tickets_{array_field}/"
    )
    flattened_df.write.mode("overwrite").parquet(output_path_array)

    # Get schema and compare
    schema = flattened_df.schema
    columns_with_types = []

    for field in schema.fields:
        if isinstance(field.dataType, IntegerType):
            columns_with_types.append({"name": field.name, "type": "INT"})
        elif isinstance(field.dataType, DateType):
            columns_with_types.append({"name": field.name, "type": "DATETIME"})
        elif isinstance(field.dataType, StringType):
            columns_with_types.append({"name": field.name, "type": "NVARCHAR(MAX)"})
        else:
            columns_with_types.append({"name": field.name, "type": "NVARCHAR(MAX)"})

    # Read original schema from ADLS for array fields
    input_path_array = (
        f"abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/"
        f"schema/intercom_tickets_schema/intercom_tickets_{array_field}/*.json"
    )
    df_array_field_schema = spark.read.option("multiline", "true").json(input_path_array)
    json_str = df_array_field_schema.select("columns").collect()[0]["columns"]
    original_columns_list = json.loads(json_str)

    # Convert lists to sets for comparison
    columns_with_types_set = {(item['name'], item['type']) for item in columns_with_types}
    original_columns_list_set = {(item['name'], item['type']) for item in original_columns_list}

    # Calculate the difference
    difference = columns_with_types_set - original_columns_list_set

    # Convert the difference back to a list of dictionaries
    difference_list = [{'name': name, 'type': type_} for name, type_ in difference]

    if difference_list:
        new_columns = [f"{col['name']} {col['type']}" for col in difference_list]

        # Generate ALTER TABLE statement without parentheses
        add_column_ddl_query = f"ALTER TABLE intercom_tickets_{array_field} ADD {', '.join(new_columns)};"
        add_column_ddl.append(add_column_ddl_query)

        # Replace old schema with new schema
        final_columns = original_columns_list + difference_list
        output_path_schema = (
            f"abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/"
            f"schema/intercom_tickets_schema/intercom_tickets_{array_field}/"
        )

        # Convert final_columns to JSON string
        new_schema_json = json.dumps(final_columns)

        # Create a DataFrame with the JSON string (wrapped in a tuple within a list to match the DataFrame format)
        df_new_schema = spark.createDataFrame([(new_schema_json,)], ["columns"])

        # Coalesce the DataFrame to a single partition
        df_new_schema_single_partition = df_new_schema.coalesce(1)

        # Save the JSON string to ADLS
        df_new_schema_single_partition.write.mode("overwrite").json(output_path_schema)


# In[12]:


dummy_query=["SELECT TOP(1) * FROM [dbo].[intercom_tickets_contacts];"]


# In[13]:


# Check if add_column_ddl is not empty
if add_column_ddl:
    # Convert add_column_ddl to JSON string
    ddl_json = json.dumps(add_column_ddl)

    # Create a DataFrame with the JSON string
    df_ddl = spark.read.json(spark.sparkContext.parallelize([ddl_json]))

    # Define the destination path in ADLS
    output_path = f"abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/incremental_ddl/intercom_tickets_ddl"

    # Coalesce the DataFrame to a single partition
    df_ddl_single_partition = df_ddl.coalesce(1)

    # Save the JSON string to ADLS
    df_ddl_single_partition.write.mode("overwrite").json(output_path)
else:
    print("No new columns to add, skipping DDL creation.")
      # Convert add_column_ddl to JSON string
    ddl_json = json.dumps(dummy_query)

    # Create a DataFrame with the JSON string
    df_ddl = spark.read.json(spark.sparkContext.parallelize([ddl_json]))

    # Define the destination path in ADLS
    output_path = f"abfss://intercom-tickets@nourishinternal.dfs.core.windows.net/incremental_ddl/intercom_tickets_ddl"

    # Coalesce the DataFrame to a single partition
    df_ddl_single_partition = df_ddl.coalesce(1)

    # Save the JSON string to ADLS
    df_ddl_single_partition.write.mode("overwrite").json(output_path)

