#!/bin/bash

# Run Basic Reporting Notebook on Snowflake using CREATE NOTEBOOK FROM stage
# This script uploads files to a stage then creates notebook via SQL
#
# Usage: ./run_snowpark_pipeline_notebook.sh [connection-name]
#   connection-name: Optional Snowflake connection from ~/.snowflake/config.toml
#                    If not specified, uses the default connection

# Display help message
show_help() {
    cat << EOF
Run Basic Reporting Notebook on Snowflake (CREATE NOTEBOOK FROM Stage)
=========================================================================

Executes the Basic Reporting Notebook using SQL CREATE NOTEBOOK FROM stage.
Files in stage directory are accessible to notebook for pd.read_csv().

Usage:
  ./run_snowpark_pipeline_notebook.sh [OPTIONS] [CONNECTION_NAME]

Options:
  --help, -h          Show this help message and exit

Arguments:
  CONNECTION_NAME     Optional Snowflake connection from ~/.snowflake/config.toml
                      If not specified, uses the default connection

Examples:
  ./run_snowpark_pipeline_notebook.sh                      # Use default connection
  ./run_snowpark_pipeline_notebook.sh migration-data-2     # Use specific connection
  ./run_snowpark_pipeline_notebook.sh --help               # Show this help

Prerequisites:
  - Snow CLI installed and configured
  - AdventureWorks2017 database and schemas (run ./setup_snowflake.sh first)
  - COMPUTE_WH warehouse (Snowpark-optimized recommended)
  - Config files: spark_configs.txt and sql_server_credentials.txt
  - DIMCUSTOMER table with data in Snowflake
  - Notebook must use: from snowflake import snowpark_connect

Approach:
  1. Upload notebook and config files to custom stage
  2. Create notebook using CREATE NOTEBOOK FROM stage
  3. Files in stage directory are accessible to notebook
  4. Execute notebook (requires Runtime 2.0 for snowpark-connect)

Configuration:
  Database:      AdventureWorks2017
  Schema:        PUBLIC
  Stage:         reporting_stage
  Warehouse:     COMPUTE_WH (both query and runtime)
  Packages:      snowpark-connect, pandas, matplotlib (pre-installed)

Output:
  - Notebook execution logs
  - Summary statistics and visualizations in Snowsight

EOF
    exit 0
}

# Check for help flag
if [[ "$1" == "--help" ]] || [[ "$1" == "-h" ]]; then
    show_help
fi

# Ensure we're in the correct directory
cd "$(dirname "$0")"

# Source common Snowflake connection handling
source lib/snowflake_connection.sh

# Snowflake configuration
SNOWFLAKE_DATABASE="AdventureWorks2017"
SNOWFLAKE_SCHEMA="PUBLIC"
STAGE_NAME="reporting_stage"
NOTEBOOK_NAME="Basic Reporting Notebook - Snowflake.ipynb"
NOTEBOOK_PATH="source_code/${NOTEBOOK_NAME}"
NOTEBOOK_ID="REPORTING_NOTEBOOK"

echo "======================================================="
echo "Running Basic Reporting Notebook on Snowflake"
echo "Using SQL CREATE NOTEBOOK FROM Stage Approach"
echo "======================================================="
echo ""

# Get and validate connection
get_snowflake_connection "${1}"
if ! validate_connection; then
    exit 1
fi

# Display connection info
display_connection_info

echo "Database: ${SNOWFLAKE_DATABASE}"
echo "Schema: ${SNOWFLAKE_SCHEMA}"
echo "Stage: ${STAGE_NAME}"
echo "Query Warehouse: COMPUTE_WH"
echo "Notebook: ${NOTEBOOK_NAME}"
echo "=========================================="
echo ""

# Verify notebook file exists
if [ ! -f "${NOTEBOOK_PATH}" ]; then
    echo "❌ Error: Notebook file not found: ${NOTEBOOK_PATH}"
    exit 1
fi

# Verify config files exist
if [ ! -f "source_code/spark_configs.txt" ]; then
    echo "❌ Error: source_code/spark_configs.txt not found"
    exit 1
fi

if [ ! -f "source_code/sql_server_credentials.txt" ]; then
    echo "❌ Error: source_code/sql_server_credentials.txt not found"
    exit 1
fi

echo "✅ All required files verified"
echo ""

# Step 1: Create stage
echo "Step 1: Creating stage for notebook files..."
echo "-------------------------------------------"
snow sql \
    --connection "${EFFECTIVE_CONNECTION}" \
    --database "${SNOWFLAKE_DATABASE}" \
    --schema "${SNOWFLAKE_SCHEMA}" \
    -q "CREATE STAGE IF NOT EXISTS ${STAGE_NAME}"

if [ $? -ne 0 ]; then
    echo "❌ Failed to create stage"
    exit 1
fi

echo "✅ Stage ${STAGE_NAME} ready"
echo ""

# Step 2: Upload files to stage (all in same directory)
echo "Step 2: Uploading notebook and config files to stage..."
echo "-------------------------------------------"

# Upload notebook
echo "Uploading notebook..."
snow stage copy \
    "${NOTEBOOK_PATH}" \
    "@${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME}/" \
    --connection "${EFFECTIVE_CONNECTION}" \
    --overwrite

if [ $? -ne 0 ]; then
    echo "❌ Failed to upload notebook"
    exit 1
fi

# Upload environment.yml for package dependencies
echo "Uploading environment.yml..."
snow stage copy \
    "source_code/environment.yml" \
    "@${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME}/" \
    --connection "${EFFECTIVE_CONNECTION}" \
    --overwrite

if [ $? -ne 0 ]; then
    echo "❌ Failed to upload environment.yml"
    exit 1
fi

# Upload config files
echo "Uploading spark_configs.txt..."
snow stage copy \
    "source_code/spark_configs.txt" \
    "@${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME}/" \
    --connection "${EFFECTIVE_CONNECTION}" \
    --overwrite

if [ $? -ne 0 ]; then
    echo "❌ Failed to upload spark_configs.txt"
    exit 1
fi

echo "Uploading sql_server_credentials.txt..."
snow stage copy \
    "source_code/sql_server_credentials.txt" \
    "@${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME}/" \
    --connection "${EFFECTIVE_CONNECTION}" \
    --overwrite

if [ $? -ne 0 ]; then
    echo "❌ Failed to upload sql_server_credentials.txt"
    exit 1
fi

echo "✅ All files uploaded to @${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME}/"
echo ""

# Step 3: Create notebook from stage
echo "Step 3: Creating notebook from stage..."
echo "-------------------------------------------"
snow sql \
    --connection "${EFFECTIVE_CONNECTION}" \
    --database "${SNOWFLAKE_DATABASE}" \
    --schema "${SNOWFLAKE_SCHEMA}" \
    -q "CREATE OR REPLACE NOTEBOOK ${NOTEBOOK_ID}
  FROM '@${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME}'
  MAIN_FILE = '${NOTEBOOK_NAME}'
  QUERY_WAREHOUSE = COMPUTE_WH
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Basic Reporting Notebook with snowpark-connect support'"

if [ $? -ne 0 ]; then
    echo "❌ Failed to create notebook"
    exit 1
fi

echo "✅ Notebook ${NOTEBOOK_ID} created from stage"
echo ""

# Step 4: List uploaded files
echo "Step 4: Verifying uploaded files..."
echo "-------------------------------------------"
snow sql \
    --connection "${EFFECTIVE_CONNECTION}" \
    --database "${SNOWFLAKE_DATABASE}" \
    --schema "${SNOWFLAKE_SCHEMA}" \
    -q "LIST @${STAGE_NAME}/"

echo ""

# Step 5: Add live version to notebook
echo "Step 5: Adding live version to notebook..."
echo "-------------------------------------------"
snow sql \
    --connection "${EFFECTIVE_CONNECTION}" \
    --database "${SNOWFLAKE_DATABASE}" \
    --schema "${SNOWFLAKE_SCHEMA}" \
    -q "ALTER NOTEBOOK ${NOTEBOOK_ID} ADD LIVE VERSION FROM LAST"

if [ $? -ne 0 ]; then
    echo "❌ Failed to add live version"
    exit 1
fi

echo "✅ Live version added to notebook"
echo ""

# Step 6: Execute notebook
echo "Step 6: Executing notebook..."
echo "-------------------------------------------"
echo "⚠️  Note: environment.yml uploaded for package dependencies"
echo "       Config files accessed via pd.read_csv('spark_configs.txt')"
echo ""
snow notebook execute ${NOTEBOOK_ID} \
    --connection "${EFFECTIVE_CONNECTION}" \
    --database "${SNOWFLAKE_DATABASE}" \
    --schema "${SNOWFLAKE_SCHEMA}"

EXEC_EXIT=$?

echo ""
echo "=========================================="
if [ $EXEC_EXIT -eq 0 ]; then
    echo "✅ Notebook executed successfully!"
    echo "=========================================="
    echo ""
    echo "Deployment details:"
    echo "  Notebook: ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${NOTEBOOK_ID}"
    echo "  Method: CREATE NOTEBOOK FROM stage"
    echo "  Warehouse: COMPUTE_WH (query + runtime)"
    echo ""
    echo "Files on stage (accessible to notebook):"
    echo "  ✓ ${NOTEBOOK_NAME}"
    echo "  ✓ environment.yml (package dependencies)"
    echo "  ✓ spark_configs.txt"
    echo "  ✓ sql_server_credentials.txt"
    echo ""
    echo "Package dependencies (from environment.yml):"
    echo "  ✓ snowpark-connect (Snowpark Connect for Apache Spark)"
    echo "  ✓ pandas, matplotlib"
    echo ""
else
    echo "❌ Notebook execution failed"
    echo "=========================================="
    echo ""
    echo "Possible reasons:"
    echo "  1. Package issue - environment.yml not loaded or snowpark-connect unavailable"
    echo "  2. File access issue - Config files not found in stage directory"
    echo "  3. Data issue - DIMCUSTOMER table missing or empty"
    echo "  4. Import issue - Check notebook uses: from snowflake import snowpark_connect"
    echo ""
    echo "Troubleshooting steps:"
    echo "  1. Open notebook in Snowsight:"
    echo "     snow notebook open ${NOTEBOOK_ID} --connection ${EFFECTIVE_CONNECTION}"
    echo ""
    echo "  2. Add missing packages via Packages menu (snowpark-connect)"
    echo ""
    echo "  3. Verify stage files:"
    echo "     snow sql -q \"LIST @${STAGE_NAME}/\" --connection ${EFFECTIVE_CONNECTION}"
    echo ""
    exit 1
fi

exit $EXEC_EXIT
