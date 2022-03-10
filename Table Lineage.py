# Databricks notebook source
# MAGIC %md
# MAGIC ### Libraries

# COMMAND ----------

import os
import pandas as pd
import re
import numpy as np
import itertools
import operator

from functools import reduce
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

# pd.set_option('max_colwidth', None)
# pd.set_option('max_rows', None)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Variables

# COMMAND ----------

# Enter source locations for ddl's
directory_bronze = '/dbfs/mnt/mountedpath/'
dir_bronze = 'dbfs:/mnt/mountedpath/'

directory_silver = '/dbfs/mnt/mountedpath/'
dir_silver = 'dbfs:/mnt/mountedpath/'

directory_gold = '/dbfs/mnt/mountedpath/'
dir_gold = 'dbfs:/mnt/mountedpath/'

# COMMAND ----------

# MAGIC %md
# MAGIC ### Functions

# COMMAND ----------

# Functions
# Extract source tables in DDL and put into list
def source_tables_from(ddl):
    """
    Extract text after 'database.' at each occurance and stop at the following space.
    Return the source table name
    """
    # remove the /* */ comments and convert all to lower
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", ddl).lower()
    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", q)
    result = list()
    get_next = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select"]:
                result.append(tok)
            get_next = False
        get_next = tok.lower() in ["from", "join"]
    return result

# Extract bronze tables in DDL and put into list
def tables_from(ddl):
    """
    Extract text after 'bronze.' / 'silver.' / 'gold.' at each occurance and stop at the following space
    Return the following table name
    """
    # remove the /* */ comments and convert all to lower
    q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", ddl).lower()
    # remove whole line -- and # comments
    lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
    # remove trailing -- and # comments
    q = " ".join([re.split("--|#", line)[0] for line in lines])
    # split on blanks, parens and semicolons
    tokens = re.split(r"[\s)(;]+", q)
    result = list()
    get_next = False
    for tok in tokens:
        if get_next:
            if tok.lower() not in ["", "select"]:
                result.append(tok)
            get_next = False
        get_next = tok.lower() in ["from", "join"]
    return list(set([x for x in result if 'bronze.' in x or 'silver.' in x or 'gold.' in x]))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 1 - Get spark table that contains all of the columns in each table
# MAGIC #### Make sure that tables are not currently being loaded

# COMMAND ----------

# Source: https://medium.com/helmes-people/how-to-view-all-databases-tables-and-columns-in-databricks-9683b12fee10
# More or less lifted wholesale. Minor mods to exclude "default" and "staging" db's
# Have to delete old tables from DBX, otherwise will get exceptions when trying to read them in

from pyspark.sql.types import StructType

# get field name from schema (recursive for getting nested values)
def get_schema_field_name(field, parent=None):
  if type(field.dataType) == StructType:
    if parent == None:
      prt = field.name
    else:
      prt = parent+"."+field.name # using dot notation
    res = []
    for i in field.dataType.fields:
      res.append(get_schema_field_name(i, prt))
    return res
  else:
    if parent==None:
      res = field.name
    else:
      res = parent+"."+field.name
    return res
  
# flatten list, from https://stackoverflow.com/a/12472564/4920394
def flatten(S):
  if S == []:
    return S
  if isinstance(S[0], list):
    return flatten(S[0]) + flatten(S[1:])
  return S[:1] + flatten(S[1:])

# list of databases
db_list = [x[0] for x in spark.sql("SHOW DATABASES").rdd.collect()]
# explicitly remove unneeded database
db_list = [db for db in db_list if db not in ["default", "staging"]]

for i in db_list:
  spark.sql("SHOW TABLES IN {}".format(i)).createOrReplaceTempView(str(i)+"TablesList")

# create a query for fetching all tables from all databases
union_string = "SELECT database, tableName FROM "
for idx, item in enumerate(db_list):
  if idx == 0:
    union_string += str(item)+"TablesList WHERE isTemporary = 'false'"
  else:
    union_string += " UNION ALL SELECT database, tableName FROM {}".format(str(item)+"TablesList WHERE isTemporary = 'false'")
spark.sql(union_string).createOrReplaceTempView("allTables")

# full list = schema, table, column
full_list = []
for i in spark.sql("SELECT * FROM allTables").collect():
  table_name = i[0]+"."+i[1]
  table_schema = spark.sql("SELECT * FROM {}".format(table_name))
  column_list = []
  for j in table_schema.schema:
    column_list.append(get_schema_field_name(j))
  column_list = flatten(column_list)
  for k in column_list:
    full_list.append([i[0],i[1],k])
all_columns = spark.createDataFrame(full_list, schema = ['Database', 'TableName', 'ColumnName'])#.createOrReplaceTempView("allColumns")
all_columns = all_columns.withColumn('TableMapping', F.concat(F.col('Database'),F.lit('.'), F.col('TableName'))) # add table mapping column
for col in all_columns.columns:
    all_columns = all_columns.withColumn(col, F.lower(F.col(col))) # convert all to lower

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.all_columns

# COMMAND ----------

# create a new table
all_columns.write.mode("overwrite").saveAsTable('default.all_columns')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 2 - Bronze

# COMMAND ----------

# set directory and loop through lists
directory = directory_bronze
dir = dir_bronze

name_list = []
path_list = []
query_list = []

for filename in os.listdir(directory):
  if filename.endswith(".sql"):
    file_path=os.path.join(dir, filename)
    path_list.append(file_path)
    name_list.append(filename.replace('.sql','')),
    query_list.append(spark.read.text(file_path, wholetext = True).collect()[0]['value'])

# Check List lengths
if len(name_list) == len(query_list) == len(path_list):
    print("Name list, Query list, and Path list MATCH on " + str(len(name_list)) + " elements in each list, proceed to functions")
else:
    print("Dictionary length mismatch")
    print("Length of name_list: " + str(len(name_list)))
    print("Length of query_list: " + str(len(query_list)))
    print("Length of path_list: " + str(len(path_list)))

# Check List types
if type(name_list) == type(query_list) == type(path_list):
    print("Name list, Query list, and Path list MATCH on " + str(type(name_list)) + ", proceed to functions")
else:
    print("Dictionary length mismatch")
    print("Type of name_list: " + type(name_list))
    print("Type of query_list: " + type(query_list))
    print("Type of path_list: " + type(path_list))

# Create dataframe
bronze_df = pd.DataFrame(
    {'BronzePath' : path_list,
     'BronzeTableName' : name_list,
     'BronzeQuery' : query_list
    })
  
# Run functions on table
try:
  bronze_df_s = bronze_df
  bronze_df_s['tables_from'] = bronze_df_s.apply(lambda row : source_tables_from(row['BronzeQuery']), axis = 1) # run source_tables_from function on query column and create new column
  bronze_df_tbls = bronze_df_s.loc[:, ~bronze_df_s.where(bronze_df_s.astype(bool)).isna().all(axis=0)] # if there is an entierly empty column, delete it
  print("Functions ran successfully")
except Exception as e:
  print("Functions failed - error: " + str(e))
  
# Check output
try:
  if 'from_source' in bronze_df_tbls:
    bronze_df_tbls = bronze_df_tbls.explode('from_source')
  if 'tables_from' in bronze_df_tbls:
    bronze_df_tbls = bronze_df_tbls.explode('tables_from')
  print("Successfully exploded")
except Exception as e:
  print("Explode failed - error: " + str(e))
  
# Split out
try:
  tmp = bronze_df_tbls[['BronzeTableName','tables_from']]
  tmp = tmp['tables_from'].str.split('.', expand=True)
  bronze_sources = tmp.dropna()
  bronze_sources.columns = ['source', 'bronze_table']
  bronze_sources = bronze_sources.replace('"', '', regex=True)
except Excepetion as e:
  print("Split Failed - error: " + str(e))


# Add prefix
try:
  bronze_df_tbls['BronzeTableName'] = 'bronze.' + bronze_df_tbls['BronzeTableName'].astype(str)
  bronze_sources['bronze_table'] = 'bronze.' + bronze_sources['bronze_table'].astype(str)
except Excepetion as e:
  print("Prefix Addition Failed - error: " + str(e))

# convert all to lower
try:
  bronze_df_tbls = bronze_df_tbls.applymap(lambda s:s.lower() if type(s) == str else s)
  bronze_df_tbls = bronze_df_tbls.applymap(lambda s:s.lower() if type(s) == str else s)
except Excepetion as e:
  print("Conversion Failed - error: " + str(e))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.bronze_tables ;
# MAGIC DROP TABLE IF EXISTS default.bronze_sources

# COMMAND ----------

# create table
try:
  bronze_table_df = spark.createDataFrame(bronze_df_tbls)
  bronze_table_df.write.mode("overwrite").saveAsTable('default.bronze_tables')
  print("Bronze table successfully written")
except Exception as e:
  print("Table write failed - error: " + str(e))
  
# create table
try:
  bronze_sources_df = spark.createDataFrame(bronze_sources)
  bronze_sources_df.write.mode("overwrite").saveAsTable('default.bronze_sources')
  print("Bronze sources successfully written")
except Exception as e:
  print("Table write failed - error: " + str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 3 - Silver

# COMMAND ----------

# set directory and loop through lists
directory = directory_silver
dir = dir_silver

name_list = []
path_list = []
query_list = []

for filename in os.listdir(directory):
  if filename.endswith(".sql"):
    file_path=os.path.join(dir, filename)
    path_list.append(file_path)
    name_list.append(filename.replace('.sql','')),
    query_list.append(spark.read.text(file_path, wholetext = True).collect()[0]['value'])
    
# Check List lengths
if len(name_list) == len(query_list) == len(path_list):
    print("Name list, Query list, and Path list MATCH on " + str(len(name_list)) + " elements in each list, proceed to functions")
else:
    print("Dictionary length mismatch")
    print("Length of name_list: " + str(len(name_list)))
    print("Length of query_list: " + str(len(query_list)))
    print("Length of path_list: " + str(len(path_list)))

# Check List types
if type(name_list) == type(query_list) == type(path_list):
    print("Name list, Query list, and Path list MATCH on " + str(type(name_list)) + ", proceed to functions")
else:
    print("Dictionary length mismatch")
    print("Type of name_list: " + type(name_list))
    print("Type of query_list: " + type(query_list))
    print("Type of path_list: " + type(path_list))

# Create df
silver_df = pd.DataFrame(
    {'SilverPath' : path_list,
     'SilverTableName' : name_list,
     'SilverQuery' : query_list
    })

# Run functions
try:
  silver_df_b = silver_df
  silver_df_b['tables_from'] = silver_df_b.apply(lambda row : tables_from(row['SilverQuery']), axis = 1) # run source_tables_from function on query column and create new column
  silver_df_tbls = silver_df_b.loc[:, ~silver_df_b.where(silver_df_b.astype(bool)).isna().all(axis=0)] # if there is an entierly empty column, delete it
  print("Functions ran successfully")
except Exception as e:
  print("Functions failed - error: " + str(e))
  
# Use the python .explode() call to break out items in the lists to separate rows
try:
  if 'tables_from' in silver_df_tbls:
    silver_df_tbls = silver_df_tbls.explode('tables_from')
  print("Successfully exploded")
except Exception as e:
  print("Explode failed - error: " + str(e))
  
# set prefix of gold table name
try:
  silver_df_tbls['SilverTableName'] = 'silver.' + silver_df_tbls['SilverTableName'].astype(str)
except Exception as e:
  print("Prefix addition - error: " + str(e))

# convert all to lower
try:
  silver_df_tbls = silver_df_tbls.applymap(lambda s:s.lower() if type(s) == str else s)
except Exception as e:
  print("Conversion failed - error: " + str(e))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.silver_tables

# COMMAND ----------

# create table
try:
  silver_table_df = spark.createDataFrame(silver_df_tbls)
  silver_table_df.write.mode("overwrite").saveAsTable('default.silver_tables')
  print("Silver table successfully written")
except Exception as e:
  print("Table write failed - error: " + str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 4 - Gold

# COMMAND ----------

# set directory and loop through lists
directory = directory_gold
dir = dir_gold

name_list = []
path_list = []
query_list = []

for filename in os.listdir(directory):
  if filename.endswith(".sql"):
    file_path=os.path.join(dir, filename)
    path_list.append(file_path)
    name_list.append(filename.replace('.sql','')),
    query_list.append(spark.read.text(file_path, wholetext = True).collect()[0]['value'])
    
# Check List lengths
if len(name_list) == len(query_list) == len(path_list):
    print("Name list, Query list, and Path list MATCH on " + str(len(name_list)) + " elements in each list, proceed to functions")
else:
    print("Dictionary length mismatch")
    print("Length of name_list: " + str(len(name_list)))
    print("Length of query_list: " + str(len(query_list)))
    print("Length of path_list: " + str(len(path_list)))

# Check List types
if type(name_list) == type(query_list) == type(path_list):
    print("Name list, Query list, and Path list MATCH on " + str(type(name_list)) + ", proceed to functions")
else:
    print("Dictionary length mismatch")
    print("Type of name_list: " + type(name_list))
    print("Type of query_list: " + type(query_list))
    print("Type of path_list: " + type(path_list))

# Create df
gold_df = pd.DataFrame(
    {'GoldPath' : path_list,
     'GoldTableName' : name_list,
     'GoldQuery' : query_list
    })

# Run functions
try:
  gold_df_b = gold_df
  gold_df_b['tables_from'] = gold_df_b.apply(lambda row : tables_from(row['GoldQuery']), axis = 1) # run source_tables_from function on query column and create new column
  gold_df_tbls = gold_df_b
  gold_df_tbls = gold_df_tbls.loc[:, ~gold_df_tbls.where(gold_df_tbls.astype(bool)).isna().all(axis=0)] # if there is an entierly empty column, delete it
  print("Functions ran successfully")
except Exception as e:
  print("Functions failed - error: " + str(e))
  
# Use the python .explode() call to break out items in the lists to separate rows
try:
  if 'tables_from' in gold_df_tbls:
    gold_df_tbls = gold_df_tbls.explode('tables_from')
  print("Successfully exploded")
except Exception as e:
  print("Explode failed - error: " + str(e))
  
# set prefix of gold table name
try:
  gold_df_tbls['GoldTableName'] = 'gold.' + gold_df_tbls['GoldTableName'].astype(str)
except Exception as e:
  print("Prefix addition - error: " + str(e))

# convert all to lower
try:
  gold_df_tbls = gold_df_tbls.applymap(lambda s:s.lower() if type(s) == str else s)
except Exception as e:
  print("Conversion failed - error: " + str(e))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.gold_tables

# COMMAND ----------

# create table
try:
  gold_table_df = spark.createDataFrame(gold_df_tbls)
  gold_table_df.write.mode("overwrite").saveAsTable('default.gold_tables')
  print("Gold table successfully written")
except Exception as e:
  print("Table write failed - error: " + str(e))

# COMMAND ----------

# MAGIC %md
# MAGIC ### Step 5 Completed Table

# COMMAND ----------

# # Load tables - use if reading from indiviual delta tables
try:
  sources = spark.table("default.bronze_sources")
  bronze = spark.table("default.bronze_tables")
  silver = spark.table("default.silver_tables")
  gold = spark.table("default.gold_tables")
except Exception as e:
    print("Table load - error: " + str(e))

# Select columns    
try:
  source = sources[['source', 'bronze_table']]
  bronze_silver = silver[['tables_from', 'SilverTableName']]
  silver_silver = silver[['tables_from', 'SilverTableName']]
  silver_gold = gold[['tables_from', 'GoldTableName']]
  gold_gold = gold[['tables_from', 'GoldTableName']]
except Exception as e:
    print("Column selection - error: " + str(e))

# Table cleanup
try:
  source = source.filter(source.source.isNotNull())
  bronze_silver = bronze_silver.filter(bronze_silver.tables_from.isNotNull())
  silver_silver = silver_silver.filter(silver_silver.tables_from.isNotNull())
  silver_gold = silver_gold.filter(silver_gold.tables_from.isNotNull())
  gold_gold = gold_gold.filter(gold_gold.tables_from.isNotNull())
except Exception as e:
    print("Table cleaning - error: " + str(e))

# Union tables
try:
  table_lineage = [source, bronze_silver, silver_silver, silver_gold, gold_gold]
  tmp = reduce(DataFrame.union, table_lineage)
  tmp = tmp.distinct()
  dl_table_lineage = tmp.withColumnRenamed("source","Source")
  dl_table_lineage = dl_table_lineage.withColumnRenamed("bronze_table","Target")
  for col in dl_table_lineage.columns:
      dl_table_lineage = dl_table_lineage.withColumn(col, F.lower(F.col(col)))
except Exception as e:
    print("Union - error: " + str(e))

# COMMAND ----------

# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS default.dl_table_lineage

# COMMAND ----------

# create a new table
dl_table_lineage.write.mode("overwrite").saveAsTable('default.dl_table_lineage')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Example

# COMMAND ----------

