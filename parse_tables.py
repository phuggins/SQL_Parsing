#%%
# * * PACKAGE INSTALL * * 
# * --------------------------------
import os # simple os commands
import pandas as pd # only used to create final table (s)
import functions as f # custom functions
import json # used for viewing dat


# %%
# * * BRONZE TABLE PARSING * * 
# * --------------------------------
# set directory and loop through lists
directory = "folderpath/Bronze"
dir = "folderpath/Bronze"

# Instantiate empty lists
name_list = []
path_list = []
query_list = []

# For each file in the directory, we want to grab the path, name, and query
for filename in os.listdir(directory):
  if filename.endswith(".sql"):
    file_path=os.path.join(dir, filename)
    path_list.append(file_path)
    name_list.append(filename.replace('.sql',''))
    string = open(file_path,"r")
    ar_list = string.read()
    query_list.append(ar_list)

# create the main dictionary from the 3 lists above
bronze_dict = {i:[j, k] for i, j, k in zip(name_list, path_list, query_list)}

# Run custom functions on the sql script present in the dictionary
for val in bronze_dict.values():
    val.extend(f.source_tables_from(val[1]))
    
for val in bronze_dict.values():
    val.extend(f.tables_from(val[1]))
    
bronze_json = json.dumps(bronze_dict)

with open('json_tbl_bronze.json', 'w', encoding='utf-8') as f:
    json.dump(bronze_json, f, ensure_ascii=False, indent=4)
  
# flatten out dict into clean table for end users
bronze_df = pd.DataFrame.from_dict(bronze_dict, orient ='index')
bronze_df.drop(bronze_df.columns[[0, 1]], axis=1, inplace=True)
  
# print(bronze_df.to_string())
bronze_df.head()

# %%
# * * SILVER TABLE PARSING * * 
# * --------------------------------
# set directory and loop through lists
directory = "folderpath/Silver"
dir = "folderpaths/Silver"

# Instantiate empty lists
name_list = []
path_list = []
query_list = []

# For each file in the directory, we want to grab the path, name, and query
for filename in os.listdir(directory):
  if filename.endswith(".sql"):
    file_path=os.path.join(dir, filename)
    path_list.append(file_path)
    name_list.append(filename.replace('.sql',''))
    string = open(file_path,"r")
    ar_list = string.read()
    query_list.append(ar_list)

# create the main dictionary from the 3 lists above
silver_dict = {i:[j, k] for i, j, k in zip(name_list, path_list, query_list)}

# Run custom functions on the sql script present in the dictionary
for val in silver_dict.values():
    val.extend(f.tables_from(val[1]))
    
silver_json = json.dumps(silver_dict)

with open('json_tbl_silver.json', 'w', encoding='utf-8') as f:
    json.dump(silver_json, f, ensure_ascii=False, indent=4)
    
# flatten out dict into clean table for end users
silver_df = pd.DataFrame.from_dict(silver_dict, orient ='index')
silver_df.drop(silver_df.columns[[0, 1]], axis=1, inplace=True)
  
# print(silver_df.to_string())
silver_df.head()

# %%
# * * GOLD TABLE PARSING * * 
# * --------------------------------
# set directory and loop through lists
directory = "folderpath/Gold"
dir = "folderpaths/Gold"

# Instantiate empty lists
name_list = []
path_list = []
query_list = []

# For each file in the directory, we want to grab the path, name, and query
for filename in os.listdir(directory):
  if filename.endswith(".sql"):
    file_path=os.path.join(dir, filename)
    path_list.append(file_path)
    name_list.append(filename.replace('.sql',''))
    string = open(file_path,"r")
    ar_list = string.read()
    query_list.append(ar_list)

# create the main dictionary from the 3 lists above
gold_dict = {i:[j, k] for i, j, k in zip(name_list, path_list, query_list)}

# Run custom functions on the sql script present in the dictionary
for val in gold_dict.values():
    val.extend(f.tables_from(val[1]))
    
gold_json = json.dumps(gold_dict)

with open('json_tbl_gold.json', 'w', encoding='utf-8') as f:
    json.dump(gold_json, f, ensure_ascii=False, indent=4)
    
# flatten out dict into clean table for end users
gold_df = pd.DataFrame.from_dict(gold_json, orient ='index')
gold_df.drop(gold_df.columns[[0, 1]], axis=1, inplace=True)
  
# print(silver_df.to_string())
gold_df.head()