#!/bin/bash
# Launch an interactive PySpark shell in the Spark cluster

echo "Launching PySpark shell in spark-master container..."
echo "====================================================="
echo ""
echo "Available Spark configurations:"
echo "  - Master: spark://spark-master:7077"
echo "  - Workers: 2 (2GB RAM, 2 cores each)"
echo "  - Python version: 3.8"
echo "  - Spark version: 3.5.3"
echo ""
echo "To exit the shell, type: exit() or press Ctrl+D"
echo "====================================================="
echo ""

docker exec -it spark-master bash -c "cd /opt/spark-work && \
  /opt/spark/bin/pyspark --master spark://spark-master:7077"

