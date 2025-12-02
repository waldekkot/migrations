#!/bin/bash
# Run the pipeline_dimcustomer.py script in the Spark cluster

set -e

echo "Starting pipeline execution..."
echo "================================"

docker exec spark-master bash -c "cd /opt/spark-work && \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --driver-class-path /opt/spark/jars/mssql-jdbc-13.2.1.jre11.jar \
  --conf spark.executor.extraClassPath=/opt/spark/jars/mssql-jdbc-13.2.1.jre11.jar \
  pipeline_dimcustomer.py"

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo "================================"
    echo "Pipeline executed successfully!"
    echo ""
    echo "Check the archived CSV files:"
    ls -lh source_code/old_versions/
else
    echo "================================"
    echo "Pipeline execution failed with exit code: $EXIT_CODE"
    exit $EXIT_CODE
fi

