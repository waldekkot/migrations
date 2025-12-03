This is a demo script for the Snoflake Virtual Hands-on: https://www.snowflake.com/en/webinars/virtual-hands-on-lab/migrate-with-confidence-snowflakes-comprehensive-toolkit-for-faster-smarter-data-and-pipeline-migrations-2025-12-03/

The demo consists of 3 parts:
1. setup of the demo environment
2. migration of database objects and data (from the source database to Snowflaake)
3. migration of example Spark-based data engineering pipeline

The migrations are performed with high-levels of automation, using Snowflake toolkit:
1. SnowConvert AI (converting data sources to Snowflake, optionally also: data migration, data validation)
2. Snowpark Migration Accelerator (Spark data pipelines conversion to Snowflake Snowpark)

Note: Snowflake also allows running Spark code directly (i.e. without conversion to Snowpark) using: Snowpark Connect for Apache Spark functionality

Important links:
1. docs.snowflake.com
2. quickstarts.snowflake.com

0. PREREQUISITES
  - Docker (or other Linux containers client)
    - optional: Docker Desktop (if you like/need a UI)
  - VS Code
    - with SQL Server extension
    - with Snowflake extension
  - Snowflake CLI (https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)
  - SnowConvert AI (https://docs.snowflake.com/en/migrations/snowconvert-docs/overview)
  - Snowpark Migration Accelerator (https://docs.snowflake.com/en/migrations/sma-docs/README)
  - trial Snowflake account (https://trial.snowflake.com/)
    - any cloud provider (preferred: AWS)
    - any region (preferred: European)
    - Enterprise Edition is sufficient


1. start with cloning the provided git repo on GitHub
  1. example: cd ~/projects/quickstarts
  2. git clone https://github.com/waldekkot/migrations
  3. cd migrations


-----------------------------------------------------------------------------------------------------------------------
A. ENVIRONMENT SETUP

1. run:   . ./setenv.sh
  - this sets up some basic environment variables (like under what user/password the SQL Server in Docker will run)
    - default SQL Server username/password: sa / VHOL12345!demo

2. run local Docker container with SQL Server

# SQL Server 2017
docker run -d \
  --platform linux/amd64 \
  --name sqlserver2017 \
  -e 'ACCEPT_EULA=Y' \
  -e "MSSQL_SA_PASSWORD=$ADMIN_PASS" \
  -e 'MSSQL_PID=Developer' \
  -p 1433:1433 \
  -v "$(pwd)/AdventureWorks:/var/opt/mssql/backup" \
  -v sqlserver-data-2017:/var/opt/mssql \
  mcr.microsoft.com/mssql/server:2017-latest

# you can also use other SQL Server images
# mcr.microsoft.com/mssql/server:2022-latest
# mcr.microsoft.com/mssql/server:2019-latest
# mcr.microsoft.com/mssql/server:2017-latest

# test connectivity
sqlcmd -S 127.0.0.1,1433 -U sa -P "$ADMIN_PASS" -C -Q "SELECT name FROM sys.databases"

3. use VS Code
  a. open the repo from command line: code .
  b. install (if needed) the extensions: 
    - Snowflake
    - SQL Server
    - Python
    - Docker (optional)
  c. check the VS Code settings
    - there is UI, but sometimes the best control will give you opening the settings as JSON file
      - for example: for Snowflake extension, you can manually set LLM models used by SnowConvert AI and SMA (preferred: claude-sonnet-4-5)
      - example:
          "snowflake.smaMigrationAssistant.modelPreference": [
              "claude-sonnet-4-5"
              // "Claude 4 Sonnet - Experimental (Improved quality over 3.7 Sonnet)"
          ],
          "snowflake.snowConvertMigrationAssistant.enabled": true,
          "snowflake.snowConvertMigrationAssistant.modelPreference": [
              "claude-sonnet-4-5"
          ],

  d. configure connection to SQL Server using the SQL Server extension
    - server: 127.0.0.1, port: 1433
    - username: sa
    - password: VHOL12345!demo (or whatever you specified in ./setenv.sh)
    - database: master
    - test connectivity
  e. sample database: AdventureWorks
    - any version will be OK (here we use AW 2017)
    - SQL Server - AdventureWorks 2017 sample database
      - https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/adventure-works
      - https://github.com/Microsoft/sql-server-samples/releases/tag/adventureworks
      - already downloaded and available in: AdventureWorks directory (AdventureWorks2017.bak)
    - to restore:
      1. open script: restore-AdventureWorks-sample.sql
        1. switch the script from Snowflake format to MS SQL format (VS Code status line)
        2. enable: >: MS SQL: Toggle SQLCMD mode
        3. run code in: restore-AdventureWorks-sample.sql
          1. verify: show some data
        4. refresh the list of databases (AdventureWorks2017 should be visible there)
    - the full DDL for the AdventureWorks2017 database is in: instawdb.sql file in: AdventureWorks-oltp-install-script

  f. verify if the sample UI ("sales dashboard", built using open source Streamlit) works for the local SQL Server db
    - open a new terminal and navigate to the repo
    - source ./setenv.sh (or: . ./setenv.sh)
    - cd ui
    - cd streamlit-sqlserver
    - install uv (much better/faster Python package manager than pip)
        - https://docs.astral.sh/uv/getting-started/installation/
        - curl -LsSf https://astral.sh/uv/install.sh | sh
        - uv --version
    - run: uv run streamlit run app.py
    - a browser should open
    - the sample UI uses: Streamlit, pyodbc, 

  g. configure your trial Snowflake account
    - go to Settings -> Authentication
    - configure: Multi-factor Authentication (e.g. Passkey)
    - generate a new token (PAT - Programmatic Access Token)
      - give it a name
      - set expiration 
      - select ACCOUNTADMIN as role for the token
      - copy the PAT token into clipboard
    - open Workspaces
      - create a new SQL script (setup.sql)
      - run (adjusting this to your preference is OK):
            CREATE OR REPLACE NETWORK POLICY my_trial_open_policy
            ALLOWED_IP_LIST = ('0.0.0.0/0')
            COMMENT = 'Open access for educational trial account';

            ALTER USER WALDEK SET NETWORK_POLICY = my_trial_open_policy;

            ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_EU';
      - recommended:
        - changing the warehouse: COMPUTE_WH to Gen2

    h. edit your connections to Snowflake
      - code ~/.snowflake/config.toml
      - change the connection to your Snowflake trial account
      - use the copied PAT token as password
      - in VS Code, use Snowflake extension to connect to the trial Snowflake account
        - check if listing databases, etc. works OK

    i. configure Snowflake CLI
      - on MacOS: brew install snowflake-cli
      - snow --version
      - snow --help
      - snow connection list
      - snow object list database —format csv | xsv 

    j. install SnowConvert AI
      - consider using Snowsight: left menu: Data Ingestion -> Migrations
    k. install Snowpark Migration Accelerator
      - consider using Snowsight: left menu: Data Ingestion -> Migrations

  3. Patching the AdvenrtureWorks database
    - at the moment some of the data types used by the sample database cannot be converted by SnowConvert AI
    - examples: hierarchyid, geography
    - the easiest solution is to change tables using those table
      - convert the columns (hierarchyid/geography) to varchar (string) - using .toString() method
    - the scripts in: _AW_modfications do that
      - find-all-hierarchyid.sql - lists all tables which have columns of type: hierarchyid
      - find-all-geography.sql - lists all tables which have columns of type: geography
      - step by step example: HumanResources.Employee-hierarchyid-column.sql
      - automated (AI-generated) conversion scripts: 
        - Production.Document-hierarchyid-column.sql
        - Person.Address-geography-column.sql
    - there are backups of the original tables created (suffix: _Backup)


  4. Use Snowconvert AI to do database (objects+data) conversions, migrations & validations
    a. open SnowConvert AI
    b. check the latest version (File menu)
    c. get the Access Code (Help menu - it will come via email, very quickly)
    d. New Project
      - Extract Code
        - select SQL Server as the data source
        - paste the access code (it will be saved in SnowConvert AI tool)
        - define connection to SQL Server - test it
        - define connection to Snowflake - test it
            - privide your Snowflake trial account details
              - account: XXXX-YYYY
              - select: PAT as the authentication method
                - username
                - PAT
            - use: COMPUTE_WH as the warehouse
            - use: SNOWFLAKE_LEARNING_DB as database (as the AdventureWorks2017 database in Snowflake does not exist yet)
            - use: PUBLIC as schema
            - use: ACCOUNTADMIN as role
        - select all objects in the source (SQL Server) database (AdventureWorks2017)
          - deselect the tables suffixed with: _Backup
            1. HumanResources.Employee_Backup
            2. Person.Address_Backup
            3. Production.Document_Backup
            4. Production.ProductDocument_Backup
    e. do the Extraction and get the Conversion overview
      - most important: EWIs (= issues)
        - E: Errors
        - W: Warnings
        - I: Information
      - you will need to get rid of the EWIs (using AI and/or manually)
    f. view the reports (Word document)
    g. view the extraction output (directories with files)
    h. start with tables 
      - only then: views, functions, procedures, triggers
      - go table by table

  4. Deploy
  5. Data Migration
  6. Data Validation
  7. UI (sales dashboard) in Snowflake
    - in Snowsight: create a new Streamlit app
      - in database: AdventureWorks2017, schema: PUBLIC
    - replace the sample code with the content of: ui / streamlit_salesperson_dashboard.py
    - Run
    - verify the dashboard numbers in Snowflake match the numbers in the dashboard on SQL Server
  8. continue conversion
    a. views
    b. functions
    c. stored procedures
    d. triggers (optional)

  
-----------------------------------------------------------------------------------------------------------------------
B. migrating data pipelines (here: Spark-based)

There are two approaches 
1. run Spark code as-is (use: Snowpark Connect for Apache Spark)
2. convert Spark code (PySpark data frames API) to Snowpark Python API (similarly for Scala/Java)

Here we start with Snowpark Connect for Apache Spark

-----------------------------------------------------------------------------------------------------------------------
C. Here we demonstrate with Spark to Snowpark code conversion 
1. using SMA (Snowpark Migration Accelerator)
2. 


-----------------------------------------------------------------------------------------------------------------------
THANK YOU !
Contact: waldemar.kot@snowflake.com