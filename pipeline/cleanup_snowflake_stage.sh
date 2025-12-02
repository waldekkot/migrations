#!/bin/bash
# Cleanup script to remove files from @csv_stage in Snowflake
# This script cleans up archived CSV files from the stage using SQL REMOVE commands
#
# Usage: ./cleanup_snowflake_stage.sh [connection-name]
#   connection-name: Optional Snowflake connection from ~/.snowflake/config.toml
#                    If not specified, uses the default connection

set -e

# Display help message
show_help() {
    cat << EOF
Snowflake Stage Cleanup Script
================================

Removes all CSV files from the @csv_stage in Snowflake, including:
  - Archived files in old_versions/ subdirectory
  - Files in the stage root directory

⚠️  WARNING: This operation is destructive and cannot be undone!

Usage:
  ./cleanup_snowflake_stage.sh [OPTIONS] [CONNECTION_NAME]

Options:
  --help, -h          Show this help message and exit

Arguments:
  CONNECTION_NAME     Optional Snowflake connection from ~/.snowflake/config.toml
                      If not specified, uses the default connection

Examples:
  ./cleanup_snowflake_stage.sh                    # Use default connection
  ./cleanup_snowflake_stage.sh migrations-demo-2  # Use specific connection
  ./cleanup_snowflake_stage.sh --help             # Show this help

What gets removed:
  - @csv_stage/old_versions/*.csv (archived files)
  - @csv_stage/customer_update*.csv (root level files)
  - All remaining *.csv files in the stage

The script will:
  1. Verify connection to Snowflake
  2. Check database and stage exist
  3. Show current files and count
  4. Prompt for confirmation before deletion
  5. Remove all CSV files
  6. Display final stage state

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
STAGE_NAME="@csv_stage"

echo "Snowflake Stage Cleanup Script"
echo "================================="

# Get connection (but don't validate yet, we'll do that after display)
get_snowflake_connection "${1}"
display_connection_info

echo "Database: ${SNOWFLAKE_DATABASE}"
echo "Schema: ${SNOWFLAKE_SCHEMA}"
echo "Stage: ${STAGE_NAME}"
echo "================================="
echo ""

# Verify Snowflake connection
if ! verify_snowflake_connection "$0"; then
    exit 1
fi

echo "Step 2: Checking if database exists..."
echo "-------------------------------------------"
# Check if database exists using SHOW TERSE (faster) with JSON output and jq
# Note: Snowflake stores database names in uppercase, so we need case-insensitive comparison
db_check_sql="SHOW TERSE DATABASES LIKE '${SNOWFLAKE_DATABASE}';"
if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
    db_exists=$(snow sql -q "${db_check_sql}" \
        --format JSON \
        --connection "${SNOWFLAKE_CONNECTION}" 2>/dev/null | \
        jq -r --arg db "${SNOWFLAKE_DATABASE}" '.[] | select(.name | ascii_upcase == ($db | ascii_upcase)) | .name' 2>/dev/null || echo "")
else
    db_exists=$(snow sql -q "${db_check_sql}" \
        --format JSON 2>/dev/null | \
        jq -r --arg db "${SNOWFLAKE_DATABASE}" '.[] | select(.name | ascii_upcase == ($db | ascii_upcase)) | .name' 2>/dev/null || echo "")
fi

if [ -z "$db_exists" ]; then
    echo ""
    echo "❌ Error: Database '${SNOWFLAKE_DATABASE}' does not exist"
    echo ""
    echo "Please run the setup script first:"
    echo "  ./setup_snowflake.sh"
    if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
        echo "  or: ./setup_snowflake.sh ${SNOWFLAKE_CONNECTION}"
    fi
    echo ""
    exit 1
fi
echo "✅ Database exists: ${SNOWFLAKE_DATABASE}"
echo ""

echo "Step 3: Checking if stage exists..."
echo "-------------------------------------------"
# Check if stage exists using SHOW TERSE (faster) with JSON output and jq
# Note: Snowflake stores stage names in uppercase, so we need case-insensitive comparison
stage_check_sql="SHOW TERSE STAGES LIKE 'csv_stage' IN ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA};"
if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
    stage_exists=$(snow sql -q "${stage_check_sql}" \
        --format JSON \
        --connection "${SNOWFLAKE_CONNECTION}" \
        --database "${SNOWFLAKE_DATABASE}" \
        --schema "${SNOWFLAKE_SCHEMA}" 2>/dev/null | \
        jq -r '.[] | select(.name | ascii_upcase == "CSV_STAGE") | .name' 2>/dev/null || echo "")
else
    stage_exists=$(snow sql -q "${stage_check_sql}" \
        --format JSON \
        --database "${SNOWFLAKE_DATABASE}" \
        --schema "${SNOWFLAKE_SCHEMA}" 2>/dev/null | \
        jq -r '.[] | select(.name | ascii_upcase == "CSV_STAGE") | .name' 2>/dev/null || echo "")
fi

if [ -z "$stage_exists" ]; then
    echo ""
    echo "❌ Error: Stage 'csv_stage' does not exist in ${SNOWFLAKE_DATABASE}.${SNOWFLAKE_SCHEMA}"
    echo ""
    echo "Please run the setup script first:"
    echo "  ./setup_snowflake.sh"
    if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
        echo "  or: ./setup_snowflake.sh ${SNOWFLAKE_CONNECTION}"
    fi
    echo ""
    exit 1
fi
echo "✅ Stage exists: ${STAGE_NAME}"
echo ""

# Function to execute SQL command
execute_sql() {
    local sql=$1
    local format=${2:-"TABLE"}  # Default to TABLE format, can override with JSON
    
    if [ -n "${SNOWFLAKE_CONNECTION}" ]; then
        snow sql -q "${sql}" \
            --format "${format}" \
            --connection "${SNOWFLAKE_CONNECTION}" \
            --database "${SNOWFLAKE_DATABASE}" \
            --schema "${SNOWFLAKE_SCHEMA}" 2>&1
    else
        snow sql -q "${sql}" \
            --format "${format}" \
            --database "${SNOWFLAKE_DATABASE}" \
            --schema "${SNOWFLAKE_SCHEMA}" 2>&1
    fi
}

# Function to list files in stage
list_stage_files() {
    execute_sql "LIST ${STAGE_NAME};"
}

echo "Step 4: Checking stage contents..."
echo "-------------------------------------------"
echo "📊 Current state of ${STAGE_NAME}:"
echo ""
list_stage_files
echo ""

# Count files using JSON output and jq
file_count=$(execute_sql "LIST ${STAGE_NAME};" "JSON" 2>/dev/null | \
    jq -r '. | length' 2>/dev/null || echo "0")

# Fallback to 0 if empty or invalid
if [ -z "$file_count" ] || [ "$file_count" = "null" ]; then
    total_files=0
else
    total_files=$file_count
fi

echo "Summary:"
echo "  Total files in stage: ${total_files}"
echo ""

# Check if there are any files to delete
if [ "$total_files" -eq 0 ]; then
    echo "✅ Stage is already clean - no files to remove"
    exit 0
fi

# Confirmation prompt
read -p "⚠️  Are you sure you want to delete ALL files from the stage? (y/N): " -n 1 -r
echo ""

if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "❌ Cleanup cancelled"
    exit 0
fi

echo ""
echo "Step 5: Removing stage files..."
echo "================================="

# Remove all files using pattern matching
echo "Removing all files from ${STAGE_NAME}..."

# Remove files from old_versions subdirectory
echo ""
echo "Removing archived files from old_versions/..."
result=$(execute_sql "REMOVE ${STAGE_NAME}/old_versions/ PATTERN='.*\.csv';" || echo "No files removed")
echo "$result"

# Remove files from root level
echo ""
echo "Removing files from stage root..."
result=$(execute_sql "REMOVE ${STAGE_NAME} PATTERN='customer_update.*\.csv';" || echo "No files removed")
echo "$result"

# Alternative: Remove all CSV files
echo ""
echo "Removing any remaining CSV files..."
result=$(execute_sql "REMOVE ${STAGE_NAME} PATTERN='.*\.csv';" || echo "No files removed")
echo "$result"

echo ""
echo "================================="
echo "✅ Cleanup complete!"
echo ""
echo "📊 Final state of ${STAGE_NAME}:"
echo ""
list_stage_files
echo ""
echo "Stage is now clean and ready for fresh runs."
echo ""

