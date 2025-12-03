#!/bin/bash
# Reset the pipeline by restoring the customer_update.csv file
# Optional: truncate the SQL Server table with --truncate flag

set -e

RESET_FILE="reset_source/customer_update.csv"
TARGET_FILE="./customer_update.csv"
TRUNCATE_TABLE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --truncate|-t)
            TRUNCATE_TABLE=true
            shift
            ;;
        --help|-h)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Reset the pipeline by restoring the customer_update.csv file"
            echo ""
            echo "Options:"
            echo "  --truncate, -t    Truncate the SQL Server table [AdventureWorks2017].[dbo].[DimCustomer]"
            echo "  --help, -h        Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                # Reset CSV only"
            echo "  $0 --truncate     # Reset CSV and truncate SQL Server table"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

echo "Resetting pipeline input file..."
echo "================================="

# Check if reset source file exists
if [ ! -f "$RESET_FILE" ]; then
    echo "❌ Error: Reset file not found: $RESET_FILE"
    echo ""
    echo "Please ensure the reset file exists or run:"
    echo "  cp $TARGET_FILE $RESET_FILE"
    exit 1
fi

# Backup existing file if it exists
if [ -f "$TARGET_FILE" ]; then
    TIMESTAMP=$(date +"%Y-%m-%d_%I-%M-%S")
    BACKUP_FILE="source_code/old_versions/customer_update_${TIMESTAMP}.csv"
    
    # Ensure old_versions directory exists
    mkdir -p source_code/old_versions
    
    echo "📦 Backing up existing file to: $BACKUP_FILE"
    cp "$TARGET_FILE" "$BACKUP_FILE"
fi

# Restore from reset source
echo "♻️  Restoring customer_update.csv from reset_source/"
cp "$RESET_FILE" "$TARGET_FILE"

# Verify restoration
if [ -f "$TARGET_FILE" ]; then
    FILE_SIZE=$(ls -lh "$TARGET_FILE" | awk '{print $5}')
    LINE_COUNT=$(wc -l < "$TARGET_FILE")
    
    echo "================================="
    echo "✅ CSV file reset successfully!"
    echo ""
    echo "File details:"
    echo "  - Size: $FILE_SIZE"
    echo "  - Lines: $LINE_COUNT"
else
    echo "================================="
    echo "❌ Error: Failed to restore file"
    exit 1
fi

# Truncate SQL Server table if requested
if [ "$TRUNCATE_TABLE" = true ]; then
    echo ""
    echo "================================="
    echo "🗑️  Truncating SQL Server table..."
    echo "================================="
    
    # Read SQL Server credentials
    CREDS_FILE="source_code/sql_server_credentials.txt"
    
    if [ ! -f "$CREDS_FILE" ]; then
        echo "❌ Error: Credentials file not found: $CREDS_FILE"
        exit 1
    fi
    
    # Parse credentials from file
    SQL_USER=$(grep "^User|" "$CREDS_FILE" | cut -d'|' -f2)
    SQL_PASSWORD=$(grep "^Password|" "$CREDS_FILE" | cut -d'|' -f2)
    SQL_DATABASE=$(grep "^Database|" "$CREDS_FILE" | cut -d'|' -f2)
    
    # Extract host and port from URL
    # URL format: jdbc:sqlserver://host.docker.internal:1433;databaseName=AdventureWorks2017;...
    SQL_URL=$(grep "^URL|" "$CREDS_FILE" | cut -d'|' -f2)
    SQL_HOST=$(echo "$SQL_URL" | sed -n 's/.*:\/\/\([^:]*\):.*/\1/p')
    SQL_PORT=$(echo "$SQL_URL" | sed -n 's/.*:\([0-9]*\);.*/\1/p')
    
    # If host is host.docker.internal (used by containers), use 127.0.0.1 for host machine
    if [ "$SQL_HOST" = "host.docker.internal" ]; then
        SQL_HOST="127.0.0.1"
    fi
    
    # If localhost, use 127.0.0.1 (more reliable with sqlcmd)
    if [ "$SQL_HOST" = "localhost" ]; then
        SQL_HOST="127.0.0.1"
    fi
    
    if [ -z "$SQL_HOST" ]; then
        SQL_HOST="127.0.0.1"
    fi
    
    if [ -z "$SQL_PORT" ]; then
        SQL_PORT="1433"
    fi
    
    echo "Connecting to: $SQL_HOST:$SQL_PORT"
    echo "Database: $SQL_DATABASE"
    echo "Table: dbo.DimCustomer"
    echo ""
    
    # Check if sqlcmd is available
    if ! command -v sqlcmd &> /dev/null; then
        echo "❌ Error: sqlcmd is not installed"
        echo ""
        echo "To install sqlcmd on macOS:"
        echo "  brew tap microsoft/mssql-release https://github.com/Microsoft/homebrew-mssql-release"
        echo "  brew update"
        echo "  HOMEBREW_NO_ENV_FILTERING=1 ACCEPT_EULA=Y brew install mssql-tools"
        echo ""
        echo "Add to PATH:"
        echo "  echo 'export PATH=\"/usr/local/opt/mssql-tools/bin:\$PATH\"' >> ~/.zshrc"
        exit 1
    fi
    
    # Execute truncate using local sqlcmd connecting to localhost
    # Use -C flag to trust server certificate (needed for SQL Server 2017 with ODBC Driver 18)
    sqlcmd -S "$SQL_HOST,$SQL_PORT" -U "$SQL_USER" -P "$SQL_PASSWORD" -d "$SQL_DATABASE" -C -Q "TRUNCATE TABLE dbo.DimCustomer;" -b 2>&1
    SQLCMD_EXIT=$?
    
    if [ $SQLCMD_EXIT -eq 0 ]; then
        echo "✅ Table truncated successfully!"
    else
        echo "❌ Failed to truncate table"
        echo ""
        echo "Please verify:"
        echo "  1. SQL Server is accessible"
        echo "  2. Credentials are correct"
        echo "  3. sqlcmd is installed"
        exit 1
    fi
fi

echo ""
echo "================================="
echo "✅ Pipeline reset complete!"
echo ""
echo "Ready to run: ./run_pipeline.sh"

