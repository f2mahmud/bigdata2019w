package ca.uwaterloo.cs451.a6


import org.apache.log4j.Logger
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class TrainSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val model: ScallopOption[String] = opt[String](descr = "output path", required = true)
  verify()
}

object TrainSpamClassifier {

  val log = Logger.getLogger(getClass().getName())

  // w is the weight vector (make sure the variable is within scope)
  var w: Map[Int, Double] = Map[Int, Double]()

  // Scores a document based on its list of features.
  def spamminess(features: Array[Int]): Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  // This is the main learner:
  val delta = 0.002


  def main(argv: Array[String]) {

    val args = new TrainSpamClassifierConf(argv)

    val conf = new SparkConf().setAppName("Spam Classifier Trainer")
    val sc = new SparkContext(conf)

    log.info("input : " + args.input)
    log.info("date : " + args.model)

    val fileItems = sc.textFile(args.input())
      .map(line => {
        val lineArray = line.split(" ")
        var isSpam: Double = 0.0
        if (lineArray(1).equals("spam")) {
          isSpam = 1.0
        }
        val features: Array[Int] = lineArray.slice(2, lineArray.size - 1).map(_.toInt)
        (0, (lineArray(0), isSpam, features))
      }).groupByKey(1)

    val trainedData = fileItems.foreach(item => {
      item._2.foreach(subItem => {
        subItem._3.foreach(f => {
          val prob = 1.0 / (1.0 + Math.exp(-spamminess(subItem._3)))
          if (w.contains(f)) {
            w(f) += (subItem._2 - prob) * delta
          } else {
            w += (f -> (subItem._2 - prob) * delta)
          }
        })
      })
    })

    val model = sc.parallelize(w.toSeq).saveAsTextFile(args.model())



  }


}
