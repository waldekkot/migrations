# Snowflake Migration Toolkit Demo

> Demo script for the [Snowflake Virtual Hands-on Lab: Migrate with Confidence](https://www.snowflake.com/en/webinars/virtual-hands-on-lab/migrate-with-confidence-snowflakes-comprehensive-toolkit-for-faster-smarter-data-and-pipeline-migrations-2025-12-03/)

## Overview

The demo consists of 3 parts:

1. **Setup** of the demo environment
2. **Database Migration** — migrating database objects and data from the source database to Snowflake
3. **Pipeline Migration** — migrating an example Spark-based data engineering pipeline

### Migration Tools

The migrations are performed with high levels of automation using the Snowflake toolkit:

| Tool | Purpose |
|------|---------|
| **SnowConvert AI** | Converting data sources to Snowflake, optionally also: data migration, data validation |
| **Snowpark Migration Accelerator** | Spark data pipelines conversion to Snowflake Snowpark |

> **Note:** Snowflake also allows running Spark code directly (without conversion to Snowpark) using **Snowpark Connect for Apache Spark**.

### Important Links

- [Snowflake Documentation](https://docs.snowflake.com)
- [Snowflake Quickstarts](https://quickstarts.snowflake.com)

---

## Prerequisites

### Required Software

- **Docker** (or other Linux containers client)
  - *Optional:* Docker Desktop (if you prefer a UI)
- **VS Code** with extensions:
  - SQL Server
  - Snowflake
  - Python
  - Docker *(optional)*
- **[Snowflake CLI](https://docs.snowflake.com/en/developer-guide/snowflake-cli/index)**
- **[SnowConvert AI](https://docs.snowflake.com/en/migrations/snowconvert-docs/overview)**
- **[Snowpark Migration Accelerator](https://docs.snowflake.com/en/migrations/sma-docs/README)**

### Snowflake Account

- [Trial Snowflake account](https://trial.snowflake.com/)
  - Any cloud provider (preferred: **AWS**)
  - Any region (preferred: **European**)
  - **Enterprise Edition** is sufficient

---

## Part A: Environment Setup

### 1. Clone the Repository

```bash
cd ~/projects/quickstarts
git clone https://github.com/waldekkot/migrations
cd migrations
```

### 2. Set Environment Variables

```bash
source ./setenv.sh
```

This sets up basic environment variables including SQL Server credentials:
- **Username:** `sa`
- **Password:** `VHOL12345!demo`

### 3. Run SQL Server in Docker

#### Start SQL Server 2017 Container

```bash
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
```

<details>
<summary>Alternative SQL Server versions</summary>

```bash
# SQL Server 2022
mcr.microsoft.com/mssql/server:2022-latest

# SQL Server 2019
mcr.microsoft.com/mssql/server:2019-latest
```

</details>

#### Test Connectivity

```bash
sqlcmd -S 127.0.0.1,1433 -U sa -P "$ADMIN_PASS" -C -Q "SELECT name FROM sys.databases"
```

### 4. Configure VS Code

1. Open the repo from command line:
   ```bash
   code .
   ```

2. Install required extensions (if needed):
   - Snowflake
   - SQL Server
   - Python
   - Docker *(optional)*

3. Configure VS Code settings (JSON):
   ```json
   {
     "snowflake.smaMigrationAssistant.modelPreference": [
       "claude-sonnet-4-5"
     ],
     "snowflake.snowConvertMigrationAssistant.enabled": true,
     "snowflake.snowConvertMigrationAssistant.modelPreference": [
       "claude-sonnet-4-5"
     ]
   }
   ```

### 5. Configure SQL Server Connection

Using the SQL Server extension:

| Setting | Value |
|---------|-------|
| Server | `127.0.0.1` |
| Port | `1433` |
| Username | `sa` |
| Password | `VHOL12345!demo` |
| Database | `master` |

### 6. Restore AdventureWorks Sample Database

The AdventureWorks 2017 sample database is included in the `AdventureWorks/` directory.

**Resources:**
- [AdventureWorks GitHub](https://github.com/microsoft/sql-server-samples/tree/master/samples/databases/adventure-works)
- [Download Link](https://github.com/Microsoft/sql-server-samples/releases/tag/adventureworks)

**Restore Steps:**

1. Open `restore-AdventureWorks-sample.sql`
2. Switch the script from Snowflake format to MS SQL format (VS Code status line)
3. Enable SQLCMD mode: `>: MS SQL: Toggle SQLCMD mode`
4. Execute the restore script
5. Verify by querying some data
6. Refresh the database list — `AdventureWorks2017` should be visible

> **Note:** The full DDL is available in `AdventureWorks-oltp-install-script/instawdb.sql`

### 7. Verify the Sample UI

Test the sales dashboard (built with Streamlit) against the local SQL Server:

```bash
source ./setenv.sh
cd ui/streamlit-sqlserver
```

**Install uv (Python package manager):**

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
uv --version
```

**Run the dashboard:**

```bash
uv run streamlit run app.py
```

A browser window should open automatically.

### 8. Configure Snowflake Trial Account

1. Go to **Settings → Authentication**
2. Configure **Multi-factor Authentication** (e.g., Passkey)
3. Generate a **PAT (Programmatic Access Token)**:
   - Give it a name
   - Set expiration
   - Select `ACCOUNTADMIN` as role
   - Copy the token to clipboard

4. In Snowsight Workspaces, create and run a setup script:

   ```sql
   CREATE OR REPLACE NETWORK POLICY my_trial_open_policy
   ALLOWED_IP_LIST = ('0.0.0.0/0')
   COMMENT = 'Open access for educational trial account';

   ALTER USER WALDEK SET NETWORK_POLICY = my_trial_open_policy;

   ALTER ACCOUNT SET CORTEX_ENABLED_CROSS_REGION = 'AWS_EU';
   ```

   > **Recommended:** Change warehouse `COMPUTE_WH` to Gen2

### 9. Configure Snowflake CLI

Edit your Snowflake connections:

```bash
code ~/.snowflake/config.toml
```

Update with your trial account details and PAT token, then verify:

```bash
# Install on macOS
brew install snowflake-cli

# Verify installation
snow --version
snow --help
snow connection list
snow object list database --format csv | xsv
```

### 10. Install Migration Tools

Configure via **Snowsight → Data Ingestion → Migrations**:

- **SnowConvert AI**
- **Snowpark Migration Accelerator**

### 11. Patch the AdventureWorks Database

Some data types (`hierarchyid`, `geography`) cannot be directly converted by SnowConvert AI. The workaround is to convert these columns to `VARCHAR` using `.toString()`.

**Scripts in `_AW_modifications/`:**

| Script | Purpose |
|--------|---------|
| `find-all-hierarchyid.sql` | Lists tables with `hierarchyid` columns |
| `find-all-geography.sql` | Lists tables with `geography` columns |
| `HumanResources.Employee-hierarchyid-column.sql` | Step-by-step example |
| `Production.Document-hierarchyid-column.sql` | AI-generated conversion |
| `Person.Address-geography-column.sql` | AI-generated conversion |

> **Note:** Backups of original tables are created with `_Backup` suffix.

---

## Part B: Database Migration with SnowConvert AI

### 1. Create New Project

1. Open SnowConvert AI
2. Check for latest version (File menu)
3. Get Access Code (Help menu — delivered via email)

### 2. Extract Code

1. Select **SQL Server** as data source
2. Paste the access code
3. Configure connections:

   **SQL Server:**
   - Server: `127.0.0.1:1433`
   - Username: `sa`
   - Password: `VHOL12345!demo`

   **Snowflake:**
   - Account: `XXXX-YYYY`
   - Authentication: PAT
   - Warehouse: `COMPUTE_WH`
   - Database: `SNOWFLAKE_LEARNING_DB`
   - Schema: `PUBLIC`
   - Role: `ACCOUNTADMIN`

4. Select all objects in `AdventureWorks2017`
5. **Deselect backup tables:**
   - `HumanResources.Employee_Backup`
   - `Person.Address_Backup`
   - `Production.Document_Backup`
   - `Production.ProductDocument_Backup`

### 3. Review Conversion

Review the **EWIs** (Errors, Warnings, Information):

| Level | Description |
|-------|-------------|
| **E** | Errors — must fix |
| **W** | Warnings — should review |
| **I** | Information — optional |

### 4. Convert and Deploy

1. View reports (Word document)
2. View extraction output (directories with files)
3. Convert in order:
   - Tables (start here)
   - Views
   - Functions
   - Procedures
   - Triggers

### 5. Data Migration & Validation

1. **Deploy** converted objects to Snowflake
2. **Migrate Data** from SQL Server
3. **Validate Data** integrity

### 6. Verify with UI Dashboard

1. In Snowsight, create a new **Streamlit app**:
   - Database: `AdventureWorks2017`
   - Schema: `PUBLIC`
2. Replace sample code with content from `ui/streamlit_salesperson_dashboard.py`
3. Run and verify numbers match SQL Server dashboard

---

## Part C: Pipeline Migration (Spark to Snowpark)

### Two Approaches

1. **Run Spark as-is** using Snowpark Connect for Apache Spark
2. **Convert Spark to Snowpark** using Snowpark Migration Accelerator (SMA)

### Approach 1: Snowpark Connect for Apache Spark

#### Start Spark Cluster

```bash
source ./setenv.sh
cd pipeline-spark
docker compose up -d
```

**Containers started:**

| Container | Description |
|-----------|-------------|
| `spark-master` | Spark master node |
| `spark-worker-1` | Spark worker 1 |
| `spark-worker-2` | Spark worker 2 |
| `spark-notebook` | Jupyter Notebook (`127.0.0.1:8888`) |

> Configuration files: `docker-compose.yml`, `Dockerfile.spark`, `Dockerfile.notebook`
> Spark version: **3.5.7** with PySpark

#### Pipeline Overview

**Source:** `pipeline-spark/source_code/pipeline_dimcustomer.py`

The pipeline:
1. Reads `customer_update.csv` into PySpark DataFrame
2. Performs data transformations
3. Appends results to `AdventureWorks2017.dbo.DimCustomer`
4. Archives CSV to `old_versions/`

**Jupyter Notebook:** Uses `dbo.DimCustomer` for statistics and visualizations.

```bash
# Get notebook token
docker logs spark-notebook
```

#### Helper Scripts

| Script | Description |
|--------|-------------|
| `./cleanup.sh` | Deletes processed files from `old_versions/` and old notebook executions |
| `./reset_pipeline.sh` | Copies template CSV from `./reset_source/` |
| `./run_pipeline.sh` | Runs pipeline via `spark-submit` |
| `./run_pipeline_notebook.sh` | Runs Jupyter notebook |

### Approach 2: Spark to Snowpark Conversion with SMA

Use **Snowpark Migration Accelerator (SMA)** to convert PySpark DataFrame API code to Snowpark Python API.

---

## Thank You!

**Contact:** [waldemar.kot@snowflake.com](mailto:waldemar.kot@snowflake.com)
