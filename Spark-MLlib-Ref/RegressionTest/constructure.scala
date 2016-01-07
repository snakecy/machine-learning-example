
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.classification.{LogisticRegressionWithLBFGS, LogisticRegressionModel}
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.{LinearRegressionWithSGD, LinearRegressionModel}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.mllib.regression
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, FileSystem}
import org.apache.spark.mllib.feature.{StandardScalerModel, StandardScaler}
import org.apache.spark.mllib.stat.{MultivariateStatisticalSummary, Statistics}
import org.apache.spark.mllib.optimization.{Gradient, SquaredL2Updater}
import LBFGSApi
import CensoredGradient
// stat
// import org.apacke.spark.mllib.stat.distribution.MultivariateGaussian

case class Request(is_win:Double, payingprice:Double, days:Int, hours:Int, exchange_id:Int, app_id:Int, publiser_id:String, bidfloor:Double, w:Int, h:Int, os:Int, osv:Double, model:String, connectiontype:Int, country:String, ua:String, carrier:String, js:Int, user:Int, carriername:String, app_cat:String, btype:String, mimes:String, badv:String, bcat:String)


// main function
object TestTrainer extends Serializable{
  def main(args: Array[String]): Unit = {
    val sc = new SparkContext(new SparkConf().setAppName("example-LBFGS-example"))
    // Load training data in LIBSVM format.
    val data = sc.textFile(sc,  "hdfs://masters/simulation/train.libsvm.txt")
    // Split data into training (60%) and test (40%).
    val splits = data.randomSplit(Array(0.6, 0.4), seed = 11L)
    val training = splits(0).cache()
    val testing = splits(1).cache()
    // create
    val parsed = data.map(line => parse(line))

    // some statistics of the data
    val payingpriceStats = Statistics.colStats(parsed.map(home => Vectors.dense(home.payingprice)))
    println("PayingPrice mean: " + payingpriceStats.mean)
    println("PayingPrice max: " + payingpriceStats.max)
    println("PayingPrice min: " + payingpriceStats.min)
    println("PayingPrice variance: " + payingpriceStats.variance)
    // standard variance
    val stdDev = Math.sqrt(payingprice.variance)

    // // Compute column summary statistics.
    // val summary: MultivariateStatisticalSummary = Statistics.colStats(observations)
    // println(summary.meana) // a dense vector containing the mean value for each column
    // println(summary.variance) // column-wise variance
    // println(summary.numNonzeros) // number of nonzeros in each column

    // First Part for win rate model using logistic regression model
    // convert to labeled data for mllib
    val lrlabelData = parsed.map{ home =>
      LabeledPoint(home.is_win, Vectors.dense(home.days, home.hours, home.exchange_id, home.app_id, home.publiser_id, home.bidfloor, home.w, home.h, home.os, home.osv, home.model, home.connectiontype, home.country, home.ua, home.carrier, home.js, home.user, home.carriername, home.app_cat, home.btype, home.mimes, home.badv, home.bcat))
    }.cache()

    // scale features to 0 mean and common variance
    val lrscaler = new StandardScaler(withMean = true, withStd = true).fit(lrlabelData.map(x => x.features))
    println("Scaler mean: "+ lrscaler.mean.toArray.mkString(","))
    println("Scaler variance: "+ lrscaler.variance.toArray.mkString(","))

    val lrscaledData = lrlabelData.map{ data =>
      LabeledPoint(data.label, lrscaler.transform(Vectors.dense(data.features.toArray)))
    }

    // Run training algorithm to build the model
    // val numIterations = 1000
    // val stepSize = 0.2
    val lrmodel = new LogisticRegressionWithLBFGS()
    // lrmodel.setIntercept(true)
    // lrmodel.optimizer
    .setNumClasses(4)
    .run(lrscaledData)
    println(" ==============> logistic regression training is finished <===============")

    // determine how well the model predicts the trained data's win rate
    val lrlabelsAndPreds = lrscaledData.map { point =>
      val prediction = lrmodel.predict(point.features)
      (point.label, prediction)
    }

    val lrpower =lrlabelsAndPreds.map{
      case(v,p) => math.pow((v-p),2)
    }
    // Mean Square Error
    val lrMSE = lrpower.reduce((a,b) => a+b) /lrpower.count()
    println("Mean Square Error : " + lrMSE)

    // persist model to HDFS
    sc.parallelize(Seq(lrmodel),1).saveAsObjectFile("hdfs:///masters/simulation/model/lr.model")
    sc.parallelize(Seq(lrscaler),1).saveAsObjectFile("hdfs:///masters/simulation/model/lrscaler.model")
    println(" ==============> logistic regression model and scale model are saved in HDFS <===============")


    // Second part for winning price model using linear regression model and censored regression model

    // filter out linear training data
    val lmfiltered = parsed.filter(home => (home.payingprice == 1))
    // conver to labeled data for mllib
    val lmlabelData = lmfiltered.map{ home =>
      LabeledPoint(home.payingprice, Vectors.dense(home.days, home.hours, home.exchange_id, home.app_id, home.publiser_id, home.bidfloor, home.w, home.h, home.os, home.osv, home.model, home.connectiontype, home.country, home.ua, home.carrier, home.js, home.user, home.carriername, home.app_cat, home.btype, home.mimes, home.badv, home.bcat))
    }.cache()

    // scala features
    val lmscaledData = lmlabelData.map{ data =>
      LabeledPoint(data.label, lrscaler.transform(Vectors.dense(data.features.toArray)))
    }

    // linear regression model for win bid of simulation data
    val numIterations = 1000
    val stepSize = 0.2
    val linearReg = new LinearRegressionWithSGD()
    linearReg.setIntercept(true)
    linearReg.optimizer
    .setNumIterations(numIterations)
    .setStepSize(stepSize)

    // run linear regression
    val lmmodel = linearReg.run(lmscaledData)
    println(" ==============> linear regression training is finished <===============")

    // determine how well the model predicts the trained data's win rate
    val lmlabelsAndPreds = lmscaledData.map { point =>
      val prediction = lmmodel.predict(point.features)
      (point.label, prediction)
    }

    val lmpower =lmlabelsAndPreds.map{
      case(v,p) => math.pow((v-p),2)
    }
    // Mean Square Error
    val lmMSE = lmpower.reduce((a,b) => a+b) /lmpower.count()
    println("Mean Square Error : " + lmMSE)
    // persist model to HDFS
    sc.parallelize(Seq(lmmodel),1).saveAsObjectFile("hdfs:///masters/simulation/model/lm.model")
    println(" ==============> linear regression model is saved in HDFS <===============")

    // winning payingprice
    val clmlabelData = parsed.map{ home =>
      LabeledPoint(home.payingprice, Vectors.dense(home.days, home.hours, home.exchange_id, home.app_id, home.publiser_id, home.bidfloor, home.w, home.h, home.os, home.osv, home.model, home.connectiontype, home.country, home.ua, home.carrier, home.js, home.user, home.carriername, home.app_cat, home.btype, home.mimes, home.badv, home.bcat))
    }.cache()

    val booleanIsWin = parsed.map{home =>
      home.is_win
    }.cache()

    val clmscaledData = clmlabelData.map{ data =>
      LabeledPoint(data.label, lrscaler.transform(Vectors.dense(data.features.toArray)))
    }

    // Run training algorithm to build the model
    val numCorrections = 10
    val convergenceTol = 1e-4
    val maxNumIterations = 50
    val regParam = 0.1
    val initialWeightsWithIntercept = Vectors.dense(new Array[Double](numFeatures + 1))

    val (weightsWithIntercept, loss) = LBFGSApi.runLBFGS(
      clmscaledData,
      booleanIsWin,   // is_win
      stdDev,   // mse
      new CensoredGradient(),
      new SquaredL2Updater(),
      numCorrections,
      convergenceTol,
      maxNumIterations,
      regParam,
      initialWeightsWithIntercept)

    val model = new LinearRegressionModel(
      Vectors.dense(weightsWithIntercept.toArray.slice(0, weightsWithIntercept.size - 1)),
      weightsWithIntercept(weightsWithIntercept.size - 1))


    // run linear regression
    val clmmodel = censroedReg.run(clmscaledData)
    println(" ==============> censored regression training is finished <===============")

    // determine how well the model predicts the trained data's win rate
    val clmlabelsAndPreds = clmscaledData.map { point =>
      val prediction = clmmodel.predict(point.features)
      (point.label, prediction)
    }

    val clmpower =clmlabelsAndPreds.map{
      case(v,p) => math.pow((v-p),2)
    }
    // Mean Square Error
    val clmMSE = clmpower.reduce((a,b) => a+b) / clmpower.count()
    println("Mean Square Error : " + clmMSE)
    // persist model to HDFS
    sc.parallelize(Seq(clmmodel),1).saveAsObjectFile("hdfs:///masters/simulation/model/clm.model")
    println(" ==============> censored regression model is saved in HDFS <===============")

    println(" Start to calculate the winning price with combining the three model above")

    val totallabelsAndPreds = totalscaledData.map { point =>
      val prediction = lrmodel.predict(point.features) * lmmodel.predict(point.features) + (1 - lrmodel.predict(point.features)) * clmmodel.predict(point.features)
      (point.label, prediction)
    }

    val totalpower = totallabelsAndPreds.map{
      case(v,p) => math.pow((v-p),2)
    }
    // Mean Square Error
    val totalMSE = totalpower.reduce((a,b) => a+b) / totalpower.count()
    println("Mean Square Error : " + totalMSE)

  }


  //
  def parse(line: String) = {
    val split = line.split('\t')
    val is_win = split(0).toDouble
    val payingprice = split(1).toDouble
    val days = split(2).toInt
    val hours = split(3).toInt
    val exchange_id = split(4).toInt
    val app_id = split(5).toInt
    val publiser_id = split(6).toString
    val bidfloor = split(7).toDouble
    val w = split(8).toInt
    val h = split(9).toInt
    val os = split(10).toInt
    val osv = split(11).toDouble
    val model = split(12).toString
    val connectiontype = split(13).toInt
    val country = split(14).toString
    val ua = split(15).toString
    val carrier = split(16).toString
    val js = split(17).toInt
    val user = split(18).toInt
    val carriername = split(19).toString
    val app_cat = split(20).toString
    val btype = split(21).toString
    val mimes = split(22).toString
    val badv = split(23).toString
    val bcat = split(24).toString

    Request(is_win, payingprice, days , hours , exchange_id , app_id , publiser_id , bidfloor , w , h , os , osv , model , connectiontype , country , ua , carrier , js , user , carriername , app_cat , btype , mimes , badv , bcat)

    // stop spark
    sc.stop()
  }
}
