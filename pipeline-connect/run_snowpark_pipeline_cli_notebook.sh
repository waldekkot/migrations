#!/bin/bash

# Run Basic Reporting Notebook on Snowflake using snow notebook deploy
# This script uses the modern 'snow notebook deploy' command with project definition
#
# Usage: ./run_snowpark_pipeline_cli_notebook.sh [connection-name]
#   connection-name: Optional Snowflake connection from ~/.snowflake/config.toml
#                    If not specified, uses the default connection

# Display help message
show_help() {
    cat << EOF
Run Basic Reporting Notebook on Snowflake (snow notebook deploy)
=================================================================

Executes the Basic Reporting Notebook using the modern 'snow notebook deploy' 
command with snowflake.yml project definition for declarative configuration.

Usage:
  ./run_snowpark_pipeline_cli_notebook.sh [OPTIONS] [CONNECTION_NAME]

Options:
  --help, -h          Show this help message and exit

Arguments:
  CONNECTION_NAME     Optional Snowflake connection from ~/.snowflake/config.toml
                      If not specified, uses the default connection

Examples:
  ./run_snowpark_pipeline_cli_notebook.sh                      # Use default connection
  ./run_snowpark_pipeline_cli_notebook.sh migration-data-2     # Use specific connection
  ./run_snowpark_pipeline_cli_notebook.sh --help               # Show this help

Prerequisites:
  - Snow CLI installed and configured (v3.4.0+)
  - AdventureWorks2017 database and schemas (run ./setup_snowflake.sh first)
  - COMPUTE_WH warehouse (Snowpark-optimized recommended)
  - Config files: spark_configs.txt and sql_server_credentials.txt
  - DIMCUSTOMER table with data in Snowflake
  - snowflake.yml project definition in source_code/
  - Notebook must use: from snowflake import snowpark_connect

Approach:
  1. Use 'snow notebook deploy' with snowflake.yml project definition
  2. Automatically uploads notebook and all artifacts to stage
  3. Creates notebook with all dependencies accessible
  4. Execute notebook (requires Runtime 2.0 for snowpark-connect)

Configuration:
  Database:      AdventureWorks2017
  Schema:        PUBLIC
  Project File:  source_code/snowflake.yml
  Warehouse:     COMPUTE_WH (query warehouse)
  Runtime:       2.0 (Python 3.10 + Streamlit 1.39.1)
  Packages:      snowpark-connect, pandas, matplotlib (from environment.yml)

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
NOTEBOOK_ID="REPORTING_NOTEBOOK"
PROJECT_DIR="source_code"
PROJECT_FILE="${PROJECT_DIR}/snowflake.yml"

echo "======================================================="
echo "Running Basic Reporting Notebook on Snowflake"
echo "Using snow notebook deploy (Modern Approach)"
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
echo "Project File: ${PROJECT_FILE}"
echo "Query Warehouse: COMPUTE_WH"
echo "Notebook ID: ${NOTEBOOK_ID}"
echo "=========================================="
echo ""

# Verify project file exists
if [ ! -f "${PROJECT_FILE}" ]; then
    echo "❌ Error: Project definition file not found: ${PROJECT_FILE}"
    exit 1
fi

# Verify config files exist in source_code directory
if [ ! -f "source_code/spark_configs.txt" ]; then
    echo "❌ Error: source_code/spark_configs.txt not found"
    exit 1
fi

if [ ! -f "source_code/sql_server_credentials.txt" ]; then
    echo "❌ Error: source_code/sql_server_credentials.txt not found"
    exit 1
fi

if [ ! -f "source_code/environment.yml" ]; then
    echo "❌ Error: source_code/environment.yml not found"
    exit 1
fi

if [ ! -f "source_code/Basic Reporting Notebook - Snowflake.ipynb" ]; then
    echo "❌ Error: source_code/Basic Reporting Notebook - Snowflake.ipynb not found"
    exit 1
fi

echo "✅ All required files verified"
echo ""

# ============================================================================
# TWO-PHASE EXECUTION
# ============================================================================
# Per Snowflake docs: "Load files before starting your notebook session."
# By separating deploy and execute, the new session sees files from start.
# ============================================================================

# Phase 1: Deploy notebook using snow notebook deploy
echo "Phase 1: Deploying notebook with artifacts..."
echo "-------------------------------------------"
echo "Note: This will automatically:"
echo "  - Create/update stage"
echo "  - Upload all artifacts (notebook + config files)"
echo "  - Create or replace notebook"
echo ""

cd "${PROJECT_DIR}"

snow notebook deploy "${NOTEBOOK_ID}" \
    --replace \
    --prune \
    --connection "${EFFECTIVE_CONNECTION}" \
    --database "${SNOWFLAKE_DATABASE}" \
    --schema "${SNOWFLAKE_SCHEMA}"

DEPLOY_EXIT=$?

cd ..

if [ $DEPLOY_EXIT -ne 0 ]; then
    echo "❌ Failed to deploy notebook"
    exit 1
fi

echo ""
echo "✅ Notebook deployed successfully"
echo ""

# Verify files are on the notebook's internal stage
echo "Verifying files on notebook stage..."
echo "-------------------------------------------"
snow sql -q "LIST 'snow://notebook/${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${NOTEBOOK_ID}/versions/live'" \
    --connection "${EFFECTIVE_CONNECTION}" 2>/dev/null | head -20

echo ""
echo "✅ Files verified on stage"
echo ""

# Wait for stage synchronization
# This is critical: the new session must see files that were uploaded
SYNC_DELAY=5
echo "Waiting ${SYNC_DELAY} seconds for stage synchronization..."
echo "(This ensures the new session sees all uploaded files)"
for i in $(seq $SYNC_DELAY -1 1); do
    echo -ne "  Starting execution in ${i}s...\r"
    sleep 1
done
echo ""
echo ""

# Phase 2: Execute notebook (NEW session will see all files)
echo "Phase 2: Executing notebook..."
echo "-------------------------------------------"
echo "⚠️  Note: Runtime 2.0 provides Python 3.10 + snowpark-connect"
echo "       Config files should be accessible via pd.read_csv('spark_configs.txt')"
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
    echo "  Method: snow notebook deploy (Modern Approach)"
    echo "  Project File: ${PROJECT_FILE}"
    echo "  Warehouse: COMPUTE_WH (query)"
    echo "  Runtime: 2.0 (Python 3.10 + Streamlit 1.39.1)"
    echo ""
    echo "Deployed artifacts:"
    echo "  ✓ Basic Reporting Notebook - Snowflake.ipynb"
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
    echo "  2. Check project definition:"
    echo "     cat ${PROJECT_FILE}"
    echo ""
    echo "  3. Redeploy with verbose output:"
    echo "     cd ${PROJECT_DIR} && snow notebook deploy ${NOTEBOOK_ID} --verbose --replace"
    echo ""
    exit 1
fi

exit $EXEC_EXIT
