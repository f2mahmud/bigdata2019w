package ca.uwaterloo.cs451.a6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input: ScallopOption[String] = opt[String](descr = "input test instances", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val model: ScallopOption[String] = opt[String](descr = "classifier model", required = true)
  val method: ScallopOption[String] = opt[String](descr = "method of merging results", required = true)
  verify()
}

object ApplyEnsembleSpamClassifier {

  def spamminess(model: RDD[(Int, (Double, Double, Double))], features: RDD[(Int, Int)], x: Int): (Double) = {

    val result = model.cogroup(features)
      .flatMap(item => {
        if (item._2._2.nonEmpty) {
          List((item._1, (item._2._1.head._1, item._2._1.head._2, item._2._1.head._3)))
        } else {
          List()
        }
      }).reduce((accum, item) => {
      (accum._1, (accum._2._1 + item._2._1, accum._2._2 + item._2._2, accum._2._3 + item._2._3))
    })

    if (x == 1) {
      (result._2._1 + result._2._2 + result._2._3) / 3
    } else {
      var finalScore = 0d
      if (result._2._1 > 1) {
        finalScore += 1
      } else {
        finalScore -= 1
      }
      if (result._2._2 > 1) {
        finalScore += 1
      } else {
        finalScore -= 1
      }
      if (result._2._3 > 1) {
        finalScore += 1
      } else {
        finalScore -= 1
      }
      finalScore
    }

  }

  def classify(sc: SparkContext, input: String, model: RDD[(Int, (Double, Double, Double))], x: Int): RDD[((String, String), Double)]
  = {
    sc.textFile(input)
      .map(line => {
        val items = line.split(" ")
        val features = items.slice(2, items.size - 1).map(item => (item.toInt, 1))
        val spamValue = spamminess(model, sc.makeRDD(features), x)
        ((items(0), items(1)), spamValue)
      })
  }

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new ApplyEnsembleSpamClassifierConf(argv)

    val conf = new SparkConf().setAppName("Spam Classifier")
    val sc = new SparkContext(conf)

    FileSystem.get(sc.hadoopConfiguration).delete(new Path(args.output()), true)

    log.info("input : " + args.input())
    log.info("output : " + args.output())
    log.info("model : " + args.model())
    log.info("method : " + args.method())

    val models = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(args.model()), false)

    log.info("getting models")

    var model1 = sc.textFile(models.next().getPath.toString)
      .map(line => {
        val items = line.substring(1, line.length - 1).split(",")
        items(0).toInt -> items(1).toDouble
      })

    var model2 = sc.textFile(models.next().getPath.toString)
      .map(line => {
        val items = line.substring(1, line.length - 1).split(",")
        items(0).toInt -> items(1).toDouble
      })

    var model3 = sc.textFile(models.next().getPath.toString)
      .map(line => {
        val items = line.substring(1, line.length - 1).split(",")
        items(0).toInt -> items(1).toDouble
      })


    val broadcastedModel = sc.broadcast(model1.cogroup(model2)
      .map(item => {
        var score1 = 0d
        var score2 = 0d
        if (item._2._1.nonEmpty) {
          score1 = item._2._1.head
        }
        if (item._2._2.nonEmpty) {
          score2 = item._2._2.head
        }
        (item._1, (score1, score2))
      })
      .cogroup(model3)
      .map(item => {
        var accumScore = (0d, 0d)
        var score3 = 0d
        if (item._2._2.nonEmpty) {
          score3 = item._2._2.head
        }
        if (item._2._1.nonEmpty) {
          accumScore = item._2._1.head
        }
        (item._1, (accumScore._1, accumScore._2, score3))
      }))

    log.info("classification")


    if (args.method().equals("average")) {

      log.info("Calculating average")

      classify(sc, args.input(), broadcastedModel.value, 1).map(item => {
        var spamOrHam = "ham"
        if (item._2 > 0) {
          spamOrHam = "spam"
        }
        (item._1, item._2, item._2, spamOrHam)

      }).saveAsTextFile(args.output())

    } else {

      log.info("Calculating vote")

      classify(sc, args.input(), broadcastedModel.value, 2)
        .map(item => {
          var spamOrHam = "ham"
          if (item._2 > 0) {
            spamOrHam = "spam"
          }

          (item._1, item._2, item._2, spamOrHam)

        }).saveAsTextFile(args.output())

    }

  }

}
