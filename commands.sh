docker exec -it bigdatafinalproject-spark-client-1 \
  /opt/spark/bin/spark-submit \
    --master spark://spark-master:7077 \
    --packages org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk-bundle:1.11.901 \
    /opt/spark/jobs/batch_data_processor.py 
