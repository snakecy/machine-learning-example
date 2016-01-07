# Predictor
 ./bin/spark-submit --class "KafkaConsumer" --executor-memory 12G --total-executor-cores 32 --master spark://example:7077 ./target/scala-2.10/*-assembly-1.0.jar example-kafka.cloudapp.net:8081,example-kafka.cloudapp.net:8082 hdfs://masters/log
