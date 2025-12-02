#!/bin/bash
# Execute the Basic Reporting Notebook and save with timestamp

set -e

NOTEBOOK_NAME="Basic Reporting Notebook - SqlServer Spark.ipynb"
NOTEBOOK_PATH="/home/jovyan/work/${NOTEBOOK_NAME}"
OUTPUT_DIR="output"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M-%S")
OUTPUT_NAME="Basic_Reporting_Notebook_${TIMESTAMP}.ipynb"
OUTPUT_PATH="/home/jovyan/work/${OUTPUT_DIR}/${OUTPUT_NAME}"

echo "Executing Jupyter Notebook..."
echo "================================="
echo "Notebook: ${NOTEBOOK_NAME}"
echo "Output: ${OUTPUT_DIR}/${OUTPUT_NAME}"
echo "================================="
echo ""

# Ensure output directory exists in container
docker exec spark-notebook mkdir -p /home/jovyan/work/${OUTPUT_DIR}

# Execute notebook and save with timestamp
echo "⚙️  Running notebook (this may take a minute)..."
docker exec spark-notebook jupyter nbconvert \
    --to notebook \
    --execute \
    --output "${OUTPUT_PATH}" \
    "${NOTEBOOK_PATH}" 2>&1 | grep -E "(Executing|Writing|Error|Exception)" || true

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    # Check if output file exists
    if docker exec spark-notebook test -f "${OUTPUT_PATH}"; then
        FILE_SIZE=$(docker exec spark-notebook ls -lh "${OUTPUT_PATH}" | awk '{print $5}')
        
        echo ""
        echo "================================="
        echo "✅ Notebook executed successfully!"
        echo ""
        echo "Output file:"
        echo "  - Name: ${OUTPUT_NAME}"
        echo "  - Size: ${FILE_SIZE}"
        echo "  - Path: ${OUTPUT_DIR}/${OUTPUT_NAME}"
        echo ""
        echo "View locally:"
        echo "  open source_code/${OUTPUT_DIR}/${OUTPUT_NAME}"
    else
        echo ""
        echo "================================="
        echo "⚠️  Warning: Output file not found"
        echo "Check logs above for errors"
        exit 1
    fi
else
    echo ""
    echo "================================="
    echo "❌ Notebook execution failed"
    echo ""
    echo "Common issues:"
    echo "  1. SQL Server not accessible"
    echo "  2. Table 'dbo.DimCustomer' is empty (run ./run_pipeline.sh first)"
    echo "  3. Credentials incorrect in sql_server_credentials.txt"
    echo ""
    echo "Check full logs:"
    echo "  docker logs spark-notebook"
    exit 1
fi





