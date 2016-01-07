import kafka.serializer.StringDecoder

import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.SparkConf
import org.apache.spark.streaming.StreamingContext._

import org.apache.spark.rdd.RDD


/**
* Consumes messages from one or more topics in Kafka and does wordcount.
* Usage: DirectKafkaWordCount <brokers> <topics>
*   <brokers> is a list of one or more Kafka brokers
*   <topics> is a list of one or more kafka topics to consume from
*
* Example:
*    $ bin/run-example streaming.DirectKafkaConsumer broker1-host:port,broker2-host:port \
*    topic1,topic2,topic3 \
*    hdfs://masters/kafks_consumer_data
*/
object KafkaConsumer {
  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println(s"""
        |Usage: DirectKafkaWordCount <brokers> <topics>
        |  <brokers> is a list of one or more Kafka brokers
        |  <topics> is a list of one or more kafka topics to consume from
        |  <output> is the path of kafka topics to save
        """.stripMargin)
        System.exit(1)
      }

      //StreamingExamples.setStreamingLogLevels()

      val Array(brokers, topics, output) = args

      // Create context with 2 second batch interval
      val sparkConf = new SparkConf().setAppName("KafkaConsumer")
      val ssc =  new StreamingContext(sparkConf, Seconds(2))

      // Create direct kafka stream with brokers and topics
      // val topicsSet = topics.split(",").toSet
      val kafkaParams = Map[String, String]("metadata.broker.list" -> brokers)

      // val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicsSet)
      val req = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("req"))
      val resp = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("resp"))
      val win = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, Set("win"))

      val req_lines = req.map{ log =>
        val BR =new ParserBase
        val parserReq = BR.parseBid(log)
        (parserBid)
      }
      req_lines.saveAsTextFiles(output, "req.log")

      resp.foreachRDD{ rdd =>
        println(new java.util.Date)
        println("Bid Resp : " + rdd.count())
      }

      val win_lines = win.map{log =>
        val BW = new ParserBase
        val parserWin =  BW.parserNotice(log)
        (parserWin)
      }

      win.foreachRDD{rdd =>
        println(new java.util.Date)
        println("Win notice : " + rdd.count())
      }
      win_lines.saveAsTextFiles(output, "win.log")


      // messages.saveAsTextFiles(output,"test")
      ssc.start()
      ssc.awaitTermination()
    }
  }
