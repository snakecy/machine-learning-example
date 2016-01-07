# Predictor
 /home/*/spark/bin/spark-submit --class "KafkaConsumer" --executor-memory 12G --total-executor-cores 32 --master spark://example:7077 /home/*/consumer-kafka/target/scala-2.10/ConsumerApplication-assembly-1.0.jar example-kafka.cloudapp.net:8081,example-kafka.cloudapp.net:9093 hdfs://masters/kafka_consumer_data/rtb_log
