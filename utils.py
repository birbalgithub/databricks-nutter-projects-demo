# Databricks notebook source
# MAGIC %pip install sqlalchemy
# MAGIC %pip install teradatasqlalchemy

# COMMAND ----------

import pandas as pd
import sqlalchemy
import teradatasqlalchemy
import logging
import smtplib
import datetime

# COMMAND ----------

global_user=""
global_password=""
global_teradata_host=""
global_DBS_PORT=""
global_database=""

# COMMAND ----------

logger = logging.getLogger("SAS_migration")
logger.setLevel(logging.DEBUG) 
file_handler = logging.FileHandler("/tmp/teradata_query_run1.log") 
logger.addHandler(file_handler)

# COMMAND ----------

def get_pushdown_engine_teradata(teradata_host=None,DBS_PORT=None,database=None,user=None,password=None):  

  if(teradata_host == None): teradata_host=global_teradata_host
  if(DBS_PORT == None): DBS_PORT=global_DBS_PORT
  if(database == None): database=global_database
  if(user == None): user=global_user
  if(password == None): password=global_password
    

  
  host = f'teradatasql://{teradata_host}/?database={database}&logmech=TD2&tmode=TERA&dbs_port={DBS_PORT}&user={user}&password={password}'
  eng = sqlalchemy.create_engine(host)
  return eng

# COMMAND ----------

def read_from_teradata(teradata_host=None,DBS_PORT=None,database=None,query=None, user=None,password=None): 
  
  if(query == None):
      raise Exception("query parameter cannot be blank")
  
  if(teradata_host == None): teradata_host=global_teradata_host
  if(DBS_PORT == None): DBS_PORT=global_DBS_PORT
  if(database == None): database=global_database
  if(user == None): user=global_user
  if(password == None): password=global_password
    
    
  df = (spark.read.format("jdbc") 
            .option("url", f"jdbc:teradata://{teradata_host}/Database={database},DBS_PORT={DBS_PORT},user={user},password={password},LOG=DEBUG") 
            .option("TMODE", "TERA") 
            .option("TCP", "KEEPALIVE") 
            .option("TCP", "NODELAY") 
            .option("COLUMN_NAME", "ON") 
            .option("SESSIONS", "8") 
            .option("REDRIVE", "4") 
            .option("RECONNECT_COUNT", "20") 
            .option("TYPE", "FAST") 
            .option("driver", "com.teradata.jdbc.TeraDriver") 
            .option("query", query)
            .load())
  
  return df

# COMMAND ----------

print("hello")
