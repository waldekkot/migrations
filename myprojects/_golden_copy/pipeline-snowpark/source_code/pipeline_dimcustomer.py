# Table update in SQL Server.
# POS generates a "new customer" export each day.
# This update is a csv file that gets generated.
# This script reads the output csv file into a 
# spark dataframe, does some simple transformations,
# then loads the table in SQL Server.

# This is a simple POC for this pipeline into the ADW warehouse.

# General Imports
import pandas as pd
import os
import shutil
import datetime

# Snowpark Imports
from snowflake.snowpark import Session
from snowflake.snowpark.functions import col, split, trim, when, lit, regexp_replace
from snowflake.snowpark.types import StringType

# Credential files (using native pandas - no Snowpark session needed)
spark_creds = pd.read_csv('spark_configs.txt', index_col=None, header=0, delimiter = "|")
sql_server_creds = pd.read_csv('sql_server_credentials.txt', index_col=None, header=0, delimiter = "|")

# Spark related credentials
driver_path = spark_creds.loc[spark_creds['Specific_Element'] == 'Driver', 'Value'].item()

# SQL Server related credentials
sql_server_user = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'User', 'Value'].item()
sql_server_password = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'Password', 'Value'].item()
sql_server_url = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'URL', 'Value'].item()
sql_server_port = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'Port', 'Value'].item()
sql_server_database = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'Database', 'Value'].item()

# Spark Session
spark = Session.builder\
    .config("connection_name", "migrations-demo")\
    .config("database", "AdventureWorks2017")\
    .app_name('SparkSqlServerExample', True)\
.getOrCreate()
spark.update_query_tag({"origin":"sf_sit","name":"sma","version":{"major":8,"minor":1,"patch":8},"attributes":{"language":"Python"}})

# Snowpark read from a local csv file.
# Note: Snowpark requires uploading to stage first (unlike PySpark which reads local files directly)
# PySpark equivalent: df = spark.read.csv('customer_update.csv', header=True, inferSchema=True)
stage_name = "@~/customer_update_stage"
spark.file.put("customer_update.csv", stage_name, auto_compress=False, overwrite=True)
df = spark.read.options({"PARSE_HEADER": True, "ENCODING": "UTF8", "SKIP_BLANK_LINES": True}).csv(stage_name)
# Normalize column names: strip quotes to get unquoted identifiers (closer to PySpark behavior)
df = df.toDF([c.strip('"') for c in df.columns])
print("Successfully read data from CSV.")
df.show(5)


# --- Transformation 1: Convert string columns to uppercase ---
# This transformation iterates through all columns in the SQL Server DataFrame.
# If a column's data type is 'string', it applies the 'upper()' function to convert all characters to uppercase.
# The result is a new DataFrame 'uppercase_df' with the transformed string columns.
df_uppercase = df.select([col(c).alias(c.upper()) for c in df.columns])
print("\nDataFrame after uppercase transformation:")
df_uppercase.show()

# Split the 'full_name' column into an array of strings
# The split function takes the column and the delimiter.
# We split by ".first:" to separate last name from first name
split_col = split(df_uppercase['NAME'], lit('.first:'))
print(split_col.getItem(0))

# Extract last name and first name from the split array
# The first element (index 0) will be the last name.
# The second element (index 1) will be the first name.
# Use trim() to remove any leading/trailing whitespace that might exist.
df_transformed = df_uppercase.withColumn("LASTNAME", trim(split_col.getItem(0))) \
                   .withColumn("FIRSTNAME", trim(split_col.getItem(1)))

# Change the name - remove the "name:last:" prefix
df_transformed = df_transformed.withColumn("NEWLASTNAME", regexp_replace("LASTNAME", "name:last:", ''))

# Drop the last name column and the name column.
df_transformed = df_transformed.drop('NAME')
df_transformed = df_transformed.drop('LASTNAME')
df_transformed = df_transformed.withColumnRenamed("NEWLASTNAME", "LASTNAME")

# Check how it looks.
df_transformed.show(1)

# Add a field from data in an existing field. 
df_transformed = df_transformed.withColumn('GENDER_SHORT', when(col('GENDER') == 'Male', lit('M')).when(col('GENDER') == 'Female', lit('F')).otherwise(col('GENDER')))

# Drop the old column name.
df_transformed = df_transformed.drop('GENDER')

# Rename a couple of columns.
df_transformed = df_transformed.withColumnRenamed("GENDER_SHORT", "GENDER")
df_transformed = df_transformed.withColumnRenamed("ADDRESS", "ADDRESSLINE1")

# Add another column that is missing in the csv export.
df_transformed = df_transformed.withColumn('ADDRESSLINE2', lit(None).cast(StringType()))

# Reorder the schema based on what the table in SQL Server is expecting.
# schema order
schema_order = ['CUSTOMERKEY','GEOGRAPHYKEY','CUSTOMERALTERNATEKEY','TITLE',
                'FIRSTNAME','MIDDLENAME','LASTNAME','NAMESTYLE',
                'BIRTHDATE','MARITALSTATUS','SUFFIX','GENDER','EMAILADDRESS',
                'YEARLYINCOME','TOTALCHILDREN','NUMBERCHILDRENATHOME',
                'ENGLISHEDUCATION','SPANISHEDUCATION','FRENCHEDUCATION',
                'ENGLISHOCCUPATION','SPANISHOCCUPATION','FRENCHOCCUPATION',
                'HOUSEOWNERFLAG','NUMBERCARSOWNED','ADDRESSLINE1','ADDRESSLINE2',
                'PHONE','DATEFIRSTPURCHASE','COMMUTEDISTANCE']

# Select the new schema.
df_transformed = df_transformed.select(schema_order)
df_transformed.show()

# Write the DataFrame to SQL Server.
# Or with additional options
df_transformed.write \
    .mode("append") \
    .option("table_type", "permanent") \
    .save_as_table("dbo.DimCustomer")

# once the data has been loaded, move the file to the backup directory.
os.makedirs('old_versions', exist_ok=True)
shutil.move(r'customer_update.csv', r'old_versions/customer_update.csv')
# Rename the newly moved file with today's dat/time
today_time = datetime.datetime.now().strftime("%Y-%m-%d_%I-%M-%S")
os.rename(r'old_versions/customer_update.csv', r'old_versions/customer_update_%s.csv'%(today_time))
