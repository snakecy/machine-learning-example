

import org.apache.spark.SparkContext
import org.apache.spark.mllib.regression.LinearRegressionWithSGD
import org.apache.spark.mllib.regression.LabeledPoint

object LinearRegressionExample {

	def main(args: Array[String]) {

		val sc = new SparkContext("spark://example-rtb01:7077", "LinearRegressionExample","~/spark/", List("target/scala-2.10.1/simple-project_2.9.3-1.0.jar"))
		// Load and parse the data
		val data = sc.textFile("hdfs://masters/train_data/")
		val parsedData = data.map { line =>
		  val parts = line.split(',')
		  LabeledPoint(parts(0).toDouble, parts(1).split(' ').map(x => x.toDouble).toArray)
		}

		// Building the model
		val numIterations = 20
		val model = LinearRegressionWithSGD.train(parsedData, numIterations)

		// Evaluate model on training examples and compute training error
		val valuesAndPreds = parsedData.map { point =>
		  val prediction = model.predict(point.features)
		  (point.label, prediction)
		}
		val MSE = valuesAndPreds.map{ case(v, p) => math.pow((v - p), 2)}.reduce(_ + _)/valuesAndPreds.count
		println("training Mean Squared Error = " + MSE)
	}
}
