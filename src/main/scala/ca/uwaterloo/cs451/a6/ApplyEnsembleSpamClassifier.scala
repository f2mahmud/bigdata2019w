package ca.uwaterloo.cs451.a6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.broadcast.Broadcast
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
      (result._2._1 + result._2._2 + result._2._3) / 3d
    } else {
      var finalScore = 0d
      if (result._2._1 > 1d) {
        finalScore += 1d
      } else {
        finalScore -= 1d
      }
      if (result._2._2 > 1d) {
        finalScore += 1d
      } else {
        finalScore -= 1d
      }
      if (result._2._3 > 1d) {
        finalScore += 1d
      } else {
        finalScore -= 1d
      }
      finalScore
    }

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


    val broadcastedModel : Broadcast[Array[(Int, (Double, Double, Double))]] = sc.broadcast(model1.cogroup(model2,model3)
      .map(item => {
        var score2: Double = 0d
        var score3: Double = 0d
        var score1: Double = 0d

        if (item._2._1.nonEmpty) {
          score1 = item._2._1.head
        }
        if (item._2._2.nonEmpty) {
          score2 = item._2._2.head
        }
        if (item._2._3.nonEmpty) {
          score3 = item._2._3.head
        }
        (item._1, (score1, score2, score3))
      }).collect())

    log.info("classification")


    if (args.method().equals("average")) {

      log.info("Calculating average")

      sc.textFile(args.input())
        .map(line => {
          val items = line.split(" ")
          val features = items.slice(2, items.size - 1).map(item => (item.toInt, 1))
          val spamValue = spamminess(sc.parallelize(broadcastedModel.value), sc.parallelize(features), 1)
          ((items(0), items(1)), spamValue)
        })
        .map(item => {
        var spamOrHam = "ham"
        if (item._2 > 0) {
          spamOrHam = "spam"
        }
        (item._1, item._2, item._2, spamOrHam)

      }).saveAsTextFile(args.output())

    } else {

      log.info("Calculating vote")

      sc.textFile(args.input())
        .map(line => {
          val items = line.split(" ")
          val features = items.slice(2, items.size - 1).map(item => (item.toInt, 1))
          val spamValue = spamminess(sc.parallelize(broadcastedModel.value), sc.parallelize(features), 2)
          ((items(0), items(1)), spamValue)
        }).map(item => {
          var spamOrHam = "ham"
          if (item._2 > 0) {
            spamOrHam = "spam"
          }

          (item._1, item._2, item._2, spamOrHam)

        }).saveAsTextFile(args.output())

    }

  }

}
