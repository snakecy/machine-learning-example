
name := "KafkaConsumer"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-streaming_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-mllib_2.10" % "1.3.1" % "provided",
  "org.apache.spark" % "spark-streaming-kafka_2.10" % "1.3.1",
  "net.minidev" % "json-smart" % "1.2"
)

mergeStrategy in assembly := {
  case m if m.toLowerCase.endsWith("manifest.mf")          => MergeStrategy.discard
  case m if m.toLowerCase.matches("meta-inf.*\\.sf$")      => MergeStrategy.discard
  case "log4j.properties"                                  => MergeStrategy.discard
  case m if m.toLowerCase.startsWith("meta-inf/services/") => MergeStrategy.filterDistinctLines
  case "reference.conf"                                    => MergeStrategy.concat
  case _                                                   => MergeStrategy.first
}
