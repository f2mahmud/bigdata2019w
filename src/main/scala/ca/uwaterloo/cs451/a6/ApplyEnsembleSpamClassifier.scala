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

  def spamminess(w: scala.collection.Map[Int, Double], features: Array[Int]): Double = {
    var score = 0d
    features.foreach(f => if (w.contains(f)) score += w(f))
    score
  }

  def classify(sc: SparkContext, input: String, model: scala.collection.Map[Int, Double]): RDD[((String, String), (Double, Double))]
  = {
    sc.textFile(input)
      .map(line => {
        val items = line.split(" ")
        val features = items.slice(2, items.size - 1).map(_.toInt)
        val spamValue = spamminess(model, features)
        ((items(0), items(1)), (spamValue, 1.0))
      })
  }

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new ApplyEnsembleSpamClassifierConf(argv)

    val conf = new SparkConf().setAppName("Spam Classifier")
    val sc = new SparkContext(conf)


    log.info("input : " + args.input())
    log.info("output : " + args.output())
    log.info("model : " + args.model())
    log.info("method : " + args.method())

    val models = FileSystem.get(sc.hadoopConfiguration).listFiles(new Path(args.model()), false)

    var model = sc.broadcast(sc.textFile(models.next().getPath.toString)
      .map(line => {
        val items = line.substring(1, line.length - 1).split(",")
        items(0).toInt -> items(1).toDouble
      }).collectAsMap())

    val results = classify(sc, args.input(), model.value)


    while (models.hasNext) {
      model = sc.broadcast(sc.textFile(models.next().getPath.getName)
        .map(line => {
          val items = line.substring(1, line.length - 1).split(",")
          items(0).toInt -> items(1).toDouble
        }).collectAsMap())

      results.union(classify(sc, args.input(), model.value))
    }

    if (args.method().equals("average")) {

      results.reduceByKey((accum, value) => {
        (accum._1 + value._1, accum._2 + value._2)
      }).map(item => {
        val spamValue: Double = item._2._1 / item._2._2
        var spamOrHam = "ham"
        if (spamValue > 0) {
          spamOrHam = "spam"
        }

        (item._1, item._2, spamValue, spamOrHam)

      })}else {

        results.map(item => {
          var spamOrHam = -1    //ham
          if (item._2._1 > 0) {   //if spam
            spamOrHam = 1
          }
          (item._1,spamOrHam)
        }).reduceByKey(_+_)
        .map(item => {
          if(item._2 > 0){
            (item._1._1, item._1._2, item._2, "spam" )
          }else{
            (item._1._1, item._1._2, item._2, "ham" )
          }
        })

      }

    results.saveAsTextFile(args.output())


    }

}
