
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.classification.LogisticRegressionModel._
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils


object Simulation{
  def main(args: Array[String]){
    val conf = new SparkConf().setAppName("SimulationApi-predict")
    val sc = new SparkContext(conf)
    // Load training data in LIBSVM format.

    val pre_indata = MLUtils.loadLibSVMFile(sc,  "hdfs://masters/train_data/predict.txt")
    val pre_data = pre_indata.cache()

    val numPre = pre_data.count()
    println(s"Predicting: $numPre.")

    // load model
    val loadlrModel = LogisticRegressionModel.load(sc, "hdfs://masters/simulation/model/lrmodel")
    val loadlmModel = LinearRegressionModel.load(sc,"hdfs://masters/simulation/model/lmmodel")
    val loadclmModel = LinearRegressionModel.load(sc, "hdfs://masters/simulation/model/clmmodel")
    // println(s"ok")
    // Compute raw scores on the test set.
    val predictionAndLabels = pre_data.map { case LabeledPoint(label, features) =>
      val prediction = loadModel.predict(features)
        (prediction, label)
    }
    println(predictionAndLabels)
    }
}
