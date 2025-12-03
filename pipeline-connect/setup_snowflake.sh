#!/bin/bash
# Setup script for Snowflake database, schema, and stage
# Creates AdventureWorks2017 database and @csv_stage if they don't exist
#
# Usage: ./setup_snowflake.sh [connection-name]
#   connection-name: Optional Snowflake connection from ~/.snowflake/config.toml
#                    If not specified, uses the default connection

set -e

# Display help message
show_help() {
    cat << EOF
Snowflake Setup Script
======================

Creates the required Snowflake infrastructure for the DimCustomer pipeline:
  - AdventureWorks2017 database
  - PUBLIC schema
  - dbo schema (for SQL Server compatibility)
  - SPARK_POOL compute pool
  - csv_stage (with encryption and directory tables)

Usage:
  ./setup_snowflake.sh [OPTIONS] [CONNECTION_NAME]

Options:
  --help, -h          Show this help message and exit

Arguments:
  CONNECTION_NAME     Optional Snowflake connection from ~/.snowflake/config.toml
                      If not specified, uses the default connection

Examples:
  ./setup_snowflake.sh                    # Use default connection
  ./setup_snowflake.sh migrations-demo-2  # Use specific connection
  ./setup_snowflake.sh --help             # Show this help

What gets created:
  ✓ Database: AdventureWorks2017
  ✓ Schema: PUBLIC (for stage)
  ✓ Schema: dbo (for DIMCUSTOMER table)
  ✓ Compute Pool: SPARK_POOL (for Snowpark workloads)
  ✓ Stage: @csv_stage (with SSE encryption and directory tables enabled)

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

# Use provided connection name, or fall back to empty string (uses snow CLI default)
SNOWFLAKE_DATABASE="AdventureWorks2017"
SNOWFLAKE_SCHEMA="PUBLIC"
STAGE_NAME="csv_stage"
COMPUTE_POOL_NAME="SPARK_POOL"

echo "=========================================="
echo "Snowflake Setup Script"
echo "=========================================="
echo ""

# Get connection
get_snowflake_connection "${1}"
display_connection_info

echo "Database: ${SNOWFLAKE_DATABASE}"
echo "Schema: ${SNOWFLAKE_SCHEMA}"
echo "Stage: @${STAGE_NAME}"
echo ""
echo "=========================================="
echo ""

# Function to execute SQL command
execute_sql() {
    local sql=$1
    local description=$2
    local use_context=${3:-true}
    
    echo "${description}"
    
    # Build snow sql command with optional database/schema context and connection
    if [ "$use_context" = "true" ] && [ -n "${SNOWFLAKE_DATABASE}" ] && [ -n "${SNOWFLAKE_SCHEMA}" ]; then
        if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
            snow sql -q "${sql}" \
                --connection "${SNOWFLAKE_CONNECTION}" \
                --database "${SNOWFLAKE_DATABASE}" \
                --schema "${SNOWFLAKE_SCHEMA}" 2>&1
        else
            snow sql -q "${sql}" \
                --database "${SNOWFLAKE_DATABASE}" \
                --schema "${SNOWFLAKE_SCHEMA}" 2>&1
        fi
    else
        if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
            snow sql -q "${sql}" \
                --connection "${SNOWFLAKE_CONNECTION}" 2>&1
        else
            snow sql -q "${sql}" 2>&1
        fi
    fi
    
    if [ $? -eq 0 ]; then
        echo "✅ Success"
    else
        echo "❌ Failed"
        return 1
    fi
    echo ""
}

# Verify Snowflake connection
if ! verify_snowflake_connection "$0"; then
    exit 1
fi

echo "Step 2: Creating database (if not exists)..."
echo "-------------------------------------------"
# Don't use context for database creation
sql="CREATE DATABASE IF NOT EXISTS ${SNOWFLAKE_DATABASE} 
    COMMENT = 'AdventureWorks 2017 database for migration demo';"
echo "Creating database ${SNOWFLAKE_DATABASE}..."
if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
    snow sql -q "${sql}" --connection "${SNOWFLAKE_CONNECTION}" 2>&1
else
    snow sql -q "${sql}" 2>&1
fi
if [ $? -eq 0 ]; then
    echo "✅ Success"
else
    echo "❌ Failed"
    exit 1
fi
echo ""

echo "Step 3: Creating schema (if not exists)..."
echo "-------------------------------------------"
# Create schema with fully qualified name (database.schema)
sql="CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}
    COMMENT = 'Public schema for AdventureWorks data';"
echo "Creating schema ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}..."
if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
    snow sql -q "${sql}" --connection "${SNOWFLAKE_CONNECTION}" 2>&1
else
    snow sql -q "${sql}" 2>&1
fi
if [ $? -eq 0 ]; then
    echo "✅ Success"
else
    echo "❌ Failed"
    exit 1
fi
echo ""

echo "Step 4: Creating dbo schema (if not exists)..."
echo "-------------------------------------------"
# Create dbo schema to match SQL Server convention
sql="CREATE SCHEMA IF NOT EXISTS ${SNOWFLAKE_DATABASE}.dbo
    COMMENT = 'DBO schema for SQL Server compatibility - contains DIMCUSTOMER table';"
echo "Creating schema ${SNOWFLAKE_DATABASE}.dbo..."
if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
    snow sql -q "${sql}" --connection "${SNOWFLAKE_CONNECTION}" 2>&1
else
    snow sql -q "${sql}" 2>&1
fi
if [ $? -eq 0 ]; then
    echo "✅ Success"
else
    echo "❌ Failed"
    exit 1
fi
echo ""

echo "Step 5: Creating compute pool for Snowpark (if not exists)..."
echo "-------------------------------------------"
# Check if compute pool exists using SHOW TERSE (faster) with JSON output and jq
# Note: Snowflake stores compute pool names in uppercase, so we need case-insensitive comparison
pool_check_sql="SHOW TERSE COMPUTE POOLS LIKE '${COMPUTE_POOL_NAME}';"
echo "Checking for compute pool ${COMPUTE_POOL_NAME}..."
if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
    pool_exists=$(snow sql -q "${pool_check_sql}" \
        --format JSON \
        --connection "${SNOWFLAKE_CONNECTION}" 2>/dev/null | \
        jq -r --arg pool "${COMPUTE_POOL_NAME}" '.[] | select(.name | ascii_upcase == ($pool | ascii_upcase)) | .name' 2>/dev/null || echo "")
else
    pool_exists=$(snow sql -q "${pool_check_sql}" \
        --format JSON 2>/dev/null | \
        jq -r --arg pool "${COMPUTE_POOL_NAME}" '.[] | select(.name | ascii_upcase == ($pool | ascii_upcase)) | .name' 2>/dev/null || echo "")
fi

if [ -z "$pool_exists" ]; then
    echo "Creating compute pool ${COMPUTE_POOL_NAME}..."
    # Create compute pool for Snowpark Connect with 2 nodes
    sql="CREATE COMPUTE POOL IF NOT EXISTS ${COMPUTE_POOL_NAME}
        MIN_NODES = 1
        MAX_NODES = 2
        INSTANCE_FAMILY = CPU_X64_XS
        AUTO_RESUME = TRUE
        INITIALLY_SUSPENDED = FALSE
        COMMENT = 'Compute pool for Snowpark Connect Apache Spark workloads';"
    
    if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
        snow sql -q "${sql}" --connection "${SNOWFLAKE_CONNECTION}" 2>&1
    else
        snow sql -q "${sql}" 2>&1
    fi
    
    if [ $? -eq 0 ]; then
        echo "✅ Success"
    else
        echo "❌ Failed"
        exit 1
    fi
else
    echo "Compute pool ${COMPUTE_POOL_NAME} already exists"
    echo "✅ Success"
fi
echo ""

echo "Step 6: Setting context to database and schema..."
echo "-------------------------------------------"
# This step is informational - actual context is set via --database --schema flags
echo "ℹ️  Context will be ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA} for subsequent operations"
echo "ℹ️  DBO schema created for DIMCUSTOMER table: ${SNOWFLAKE_DATABASE}.dbo"
echo ""

echo "Step 7: Creating stage (if not exists)..."
echo "-------------------------------------------"
# Create stage with directory tables enabled and server-side encryption
create_stage_sql="CREATE STAGE IF NOT EXISTS ${STAGE_NAME}
    ENCRYPTION = (TYPE = 'SNOWFLAKE_SSE')
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Stage for customer data CSV files and configs';"

execute_sql "${create_stage_sql}" \
    "Creating stage @${STAGE_NAME} with SSE encryption and directory tables..."

# Show current stage properties using JSON output
# Note: Snowflake property names are case-sensitive, use exact case or case-insensitive comparison
echo "Stage properties:"
if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
    stage_props=$(snow sql -q "DESC STAGE ${STAGE_NAME};" \
        --format JSON \
        --connection "${SNOWFLAKE_CONNECTION}" \
        --database "${SNOWFLAKE_DATABASE}" \
        --schema "${SNOWFLAKE_SCHEMA}" 2>/dev/null | \
        jq -r '.[] | select((.property | ascii_upcase) == "DIRECTORY" or (.property | ascii_upcase) == "ENABLE_DIRECTORY_TABLE") | "  \(.property): \(.property_value)"' 2>/dev/null || echo "")
    
    if [ -n "$stage_props" ]; then
        echo "$stage_props"
    else
        echo "  Stage created successfully"
    fi
else
    stage_props=$(snow sql -q "DESC STAGE ${STAGE_NAME};" \
        --format JSON \
        --database "${SNOWFLAKE_DATABASE}" \
        --schema "${SNOWFLAKE_SCHEMA}" 2>/dev/null | \
        jq -r '.[] | select((.property | ascii_upcase) == "DIRECTORY" or (.property | ascii_upcase) == "ENABLE_DIRECTORY_TABLE") | "  \(.property): \(.property_value)"' 2>/dev/null || echo "")
    
    if [ -n "$stage_props" ]; then
        echo "$stage_props"
    else
        echo "  Stage created successfully"
    fi
fi
echo ""

echo "Step 8: Verifying stage configuration..."
echo "-------------------------------------------"
echo "Listing stage files:"
if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
    snow stage list-files "@${STAGE_NAME}" \
        --connection "${SNOWFLAKE_CONNECTION}" \
        --database "${SNOWFLAKE_DATABASE}" \
        --schema "${SNOWFLAKE_SCHEMA}" || echo "Stage is empty (this is normal for new setup)"
else
    snow stage list-files "@${STAGE_NAME}" \
        --database "${SNOWFLAKE_DATABASE}" \
        --schema "${SNOWFLAKE_SCHEMA}" || echo "Stage is empty (this is normal for new setup)"
fi
echo ""

echo "Step 9: Testing directory table function..."
echo "-------------------------------------------"
execute_sql "SELECT COUNT(*) as file_count FROM directory(@${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}.${STAGE_NAME});" \
    "Querying directory table for @${STAGE_NAME}..."

echo "=========================================="
echo "✅ Snowflake Setup Complete!"
echo "=========================================="
echo ""
echo "Summary:"
if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
    echo "  ✓ Connection: ${SNOWFLAKE_CONNECTION}"
else
    echo "  ✓ Connection: ${DEFAULT_CONNECTION} (default)"
fi
echo "  ✓ Database: ${SNOWFLAKE_DATABASE}"
echo "  ✓ Schema: ${SNOWFLAKE_SCHEMA}"
echo "  ✓ Schema (DBO): ${SNOWFLAKE_DATABASE}.dbo"
echo "  ✓ Compute Pool: ${COMPUTE_POOL_NAME}"
echo "  ✓ Stage: @${STAGE_NAME}"
echo "  ✓ Encryption: SNOWFLAKE_SSE (Server-Side Encryption)"
echo "  ✓ Directory Tables: ENABLED"
echo "  ✓ Snowpark Connect: COMPATIBLE"
echo ""
echo "Next steps:"
echo "  1. Upload CSV files: snow stage copy <file> @${STAGE_NAME}"
echo "  2. Run pipeline: ./run_snowpark_pipeline.sh"
echo ""
echo "=========================================="

