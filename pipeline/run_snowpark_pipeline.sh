#!/bin/bash

# Run DimCustomer pipeline on Snowflake using Snowpark Connect for Apache Spark
# This script uses snowpark-submit to execute the pipeline on Snowflake infrastructure
#
# Usage: ./run_snowpark_pipeline.sh [connection-name]
#   connection-name: Optional Snowflake connection from ~/.snowflake/config.toml
#                    If not specified, uses the default connection

# Display help message
show_help() {
    cat << EOF
Run DimCustomer Pipeline on Snowflake
======================================

Executes the DimCustomer data pipeline using Snowpark Connect for Apache Spark.
This script uploads a CSV file to Snowflake stage, processes it with PySpark,
and loads the transformed data into the dbo.DIMCUSTOMER table.

Usage:
  ./run_snowpark_pipeline.sh [OPTIONS] [CONNECTION_NAME]

Options:
  --help, -h          Show this help message and exit

Arguments:
  CONNECTION_NAME     Optional Snowflake connection from ~/.snowflake/config.toml
                      If not specified, uses the default connection

Examples:
  ./run_snowpark_pipeline.sh                    # Use default connection
  ./run_snowpark_pipeline.sh migrations-demo-2  # Use specific connection
  ./run_snowpark_pipeline.sh --help             # Show this help

Prerequisites:
  - Python 3.12 virtual environment (.venv) with snowpark-submit installed
  - AdventureWorks2017 database and schemas (run ./setup_snowflake.sh first)
  - SPARK_POOL compute pool configured
  - @csv_stage created in PUBLIC schema
  - reset_source/customer_update.csv file exists

Pipeline Steps:
  1. Upload customer_update.csv to @csv_stage
  2. Submit PySpark job to Snowpark Connect
  3. Transform data (name splitting, uppercase, gender mapping)
  4. Write to dbo.DIMCUSTOMER table
  5. Archive processed CSV to @csv_stage/old_versions/

Configuration:
  Database:      AdventureWorks2017
  Schema:        PUBLIC (for stage)
  Target Schema: dbo (for DIMCUSTOMER table)
  Warehouse:     COMPUTE_WH
  Compute Pool:  SPARK_POOL

Output:
  - Pipeline execution logs
  - Stage file listing (before and after)
  - Instructions for retrieving archived files

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

# Snowflake connection configuration
SNOWFLAKE_DATABASE="AdventureWorks2017"
SNOWFLAKE_SCHEMA="PUBLIC"
SNOWFLAKE_WAREHOUSE="COMPUTE_WH"
SNOWFLAKE_COMPUTE_POOL="SPARK_POOL"

echo "=========================================="
echo "Running DimCustomer Pipeline on Snowflake"
echo "Using Snowpark Connect for Apache Spark"
echo "=========================================="
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
echo "=========================================="
echo ""

# Check if the venv is activated
if [[ -z "${VIRTUAL_ENV}" ]]; then
    echo "Activating Python 3.12 virtual environment..."
    source .venv/bin/activate
fi

# Verify snowpark-submit is available
if ! command -v snowpark-submit &> /dev/null; then
    echo "Error: snowpark-submit not found!"
    echo "Please install it with: uv pip install snowpark-submit"
    exit 1
fi

echo "Using snowpark-submit version:"
snowpark-submit --version
echo ""

# Set the workload name with timestamp
WORKLOAD_NAME="DIMCUSTOMER_PIPELINE_$(date +%Y%m%d_%H%M%S)"

echo "Workload name: ${WORKLOAD_NAME}"
echo "Warehouse: ${SNOWFLAKE_WAREHOUSE}"
echo "Compute Pool: ${SNOWFLAKE_COMPUTE_POOL}"
echo ""

# Step 1: Upload the CSV file to Snowflake stage
echo "Step 1: Uploading customer_update.csv to @csv_stage..."
snow stage copy reset_source/customer_update.csv @csv_stage \
  --overwrite \
  --connection "${EFFECTIVE_CONNECTION}" \
  --database "${SNOWFLAKE_DATABASE}" \
  --schema "${SNOWFLAKE_SCHEMA}"

if [ $? -ne 0 ]; then
    echo "❌ Failed to upload CSV file to stage"
    exit 1
fi
echo "✅ CSV file uploaded successfully"
echo ""

# Display current stage contents
echo "📁 Current files in @csv_stage:"
snow stage list-files @csv_stage \
  --connection "${EFFECTIVE_CONNECTION}" \
  --database "${SNOWFLAKE_DATABASE}" \
  --schema "${SNOWFLAKE_SCHEMA}"
echo ""

# Step 2: Submit the pipeline to Snowflake
# The CSV file was uploaded to @csv_stage in Step 1
echo "Step 2: Submitting pipeline to Snowflake..."
echo "Note: Pandas is available by default via Anaconda integration"
echo ""

# Submit using the effective connection name
snowpark-submit \
  --snowflake-connection-name "${EFFECTIVE_CONNECTION}" \
  --snowflake-workload-name "${WORKLOAD_NAME}" \
  --database "${SNOWFLAKE_DATABASE}" \
  --schema "${SNOWFLAKE_SCHEMA}" \
  --warehouse "${SNOWFLAKE_WAREHOUSE}" \
  --compute-pool "${SNOWFLAKE_COMPUTE_POOL}" \
  --snowflake-stage @csv_stage \
  --py-files source_code/spark_configs.txt,source_code/sql_server_credentials.txt \
  --wait-for-completion \
  --display-logs \
  --snowflake-log-level INFO \
  --comment "Daily DimCustomer data pipeline from POS system with Snowflake Pandas" \
  source_code/pipeline_dimcustomer_snowflake.py

# Check the exit code
EXIT_CODE=$?

echo ""
echo "=========================================="
if [ $EXIT_CODE -eq 0 ]; then
    echo "✅ Pipeline completed successfully!"
    echo "=========================================="
    echo ""
    
    # Display final stage contents after pipeline completion
    echo "📁 Final state of @csv_stage:"
    snow stage list-files @csv_stage \
      --connection "${EFFECTIVE_CONNECTION}" \
      --database "${SNOWFLAKE_DATABASE}" \
      --schema "${SNOWFLAKE_SCHEMA}"
    echo ""
    echo "=========================================="
    echo ""
    echo "ℹ️  Archived files location: @csv_stage/old_versions/"
    echo ""
    echo "To retrieve an archived file, use:"
    echo "  snow stage copy @csv_stage/old_versions/<filename> ./ \\"
    echo "    --connection ${EFFECTIVE_CONNECTION} \\"
    echo "    --database ${SNOWFLAKE_DATABASE} \\"
    echo "    --schema ${SNOWFLAKE_SCHEMA}"
    echo ""
    echo "Example:"
    # Get the most recent archived file name
    archived_file=$(snow stage list-files @csv_stage/old_versions/ \
      --connection "${EFFECTIVE_CONNECTION}" \
      --database "${SNOWFLAKE_DATABASE}" \
      --schema "${SNOWFLAKE_SCHEMA}" 2>/dev/null | \
      grep -o "old_versions/customer_update_[^|]*\.csv" | tail -1)
    
    if [ -n "$archived_file" ]; then
        echo "  snow stage copy @csv_stage/${archived_file} ./ \\"
        echo "    --connection ${EFFECTIVE_CONNECTION} \\"
        echo "    --database ${SNOWFLAKE_DATABASE} \\"
        echo "    --schema ${SNOWFLAKE_SCHEMA}"
    else
        echo "  snow stage copy @csv_stage/old_versions/customer_update_2025-11-24_12-00-00.csv ./ \\"
        echo "    --connection ${EFFECTIVE_CONNECTION} \\"
        echo "    --database ${SNOWFLAKE_DATABASE} \\"
        echo "    --schema ${SNOWFLAKE_SCHEMA}"
    fi
    echo ""
else
    echo "❌ Pipeline failed with exit code: ${EXIT_CODE}"
    echo "=========================================="
fi

exit $EXIT_CODE

