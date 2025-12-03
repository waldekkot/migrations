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

# PySpark Imports
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import upper
from pyspark.sql.functions import lower
from pyspark.sql.functions import split
from pyspark.sql.functions import trim
from pyspark.sql.functions import when
from pyspark.sql.functions import lit
from pyspark.sql.functions import expr
from pyspark.sql.functions import regexp_replace

# Credential files.
spark_creds = pd.read_csv('spark_configs.txt', index_col=None, header=0, delimiter = "|")
sql_server_creds = pd.read_csv('sql_server_credentials.txt', index_col=None, header=0, delimiter = "|")

# Spark related credentials
driver_path = spark_creds.loc[spark_creds['Specific_Element'] == 'Driver', 'Value'].item()

# SQL Server related credentials.
sql_server_user = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'User', 'Value'].item()
sql_server_password = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'Password', 'Value'].item()
sql_server_url = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'URL', 'Value'].item()
sql_server_port = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'Port', 'Value'].item()
sql_server_database = sql_server_creds.loc[sql_server_creds['Specific_Element'] == 'Database', 'Value'].item()

# Spark Session
spark = SparkSession.builder.config('spark.driver.extraClassPath', driver_path) \
                    .appName('SparkSqlServerExample') \
                    .getOrCreate()

# Spark read from Snowflake stage
# In Snowpark Connect, files are read from stages, not local filesystem
# The customer_update.csv was uploaded to @csv_stage via --py-files and --snowflake-stage
df = spark.read.csv('@csv_stage/customer_update.csv', header=True, inferSchema=True)
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
# We split by ", " (comma followed by a space)
split_col = split(df_uppercase['NAME'], '.first:')
print(split_col.getItem(0))

# Extract last name and first name from the split array
# The first element (index 0) will be the last name.
# The second element (index 1) will be the first name.
# Use trim() to remove any leading/trailing whitespace that might exist.
df_transformed = df_uppercase.withColumn("LASTNAME", trim(split_col.getItem(0))) \
                   .withColumn("FIRSTNAME", trim(split_col.getItem(1)))

# Change the name.
df_transformed = df_transformed.withColumn("NEWLASTNAME",regexp_replace("LASTNAME","name:last:",''))

# Drop the last name column and the name colum.
df_transformed = df_transformed.drop('NAME')
df_transformed = df_transformed.drop('LASTNAME')
df_transformed = df_transformed.withColumnRenamed("NEWLASTNAME", "LASTNAME")

# Check how it looks.
df_transformed.show(1)

# Add a field from data in an existing field. 
df_transformed = df_transformed.withColumn('GENDER_SHORT', when(col('GENDER') == 'Male', 
                                            lit('M')).when(col('GENDER') == 'Female', lit('F')).otherwise(col('GENDER')))

# Drop the old column name.
df_transformed = df_transformed.drop('GENDER')

# Rename a couple of columns.
df_transformed = df_transformed.withColumnRenamed("GENDER_SHORT", "GENDER")
df_transformed = df_transformed.withColumnRenamed("ADDRESS", "ADDRESSLINE1")

# Add another column that is missing in the csv export.
from pyspark.sql.types import StringType #fix: import string type to avoid null value error
df_transformed = df_transformed.withColumn('ADDRESSLINE2', lit(None).cast(StringType())) #fix: cast to string type to avoid null value error

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
df_transformed.write \
    .format("snowflake") \
    .mode("append") \
    .option("dbtable", "dbo.DIMCUSTOMER") \
    .save()

# once the data has been loaded, move the file to the backup directory.
# In Snowflake, we use COPY FILES to move/rename stage files
# Generate timestamp for archived filename
today_time = datetime.datetime.now().strftime("%Y-%m-%d_%I-%M-%S")
archived_filename = f'customer_update_{today_time}.csv'

# Enable SQL passthrough to use Snowflake-native COPY FILES command
spark.conf.set("snowpark.connect.sql.passthrough", "true")

try:
    # Copy the file from @csv_stage to @csv_stage/old_versions/ with new name
    copy_sql = f"""
        COPY FILES 
        INTO @csv_stage/old_versions/
        FROM (SELECT '@csv_stage/customer_update.csv', '{archived_filename}')
    """
    spark.sql(copy_sql).collect()
    print(f"File archived to: @csv_stage/old_versions/{archived_filename}")
    
    # Remove the original file from the stage after successful copy
    remove_sql = "REMOVE @csv_stage/customer_update.csv"
    spark.sql(remove_sql).collect()
    print("Original file removed from stage")
except Exception as e:
    print(f"Warning: File archival failed: {e}")
finally:
    # Reset passthrough to default
    spark.conf.set("snowpark.connect.sql.passthrough", "false")

# some rogue code that doesn't make any sense!