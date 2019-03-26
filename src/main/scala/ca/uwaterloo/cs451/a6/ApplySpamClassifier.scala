package ca.uwaterloo.cs451.a6


import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ApplySpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input: ScallopOption[String] = opt[String](descr = "input test instances", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required =  true)
  val model: ScallopOption[String] = opt[String](descr = "classifier model", required = true)
  verify()
}


object ApplySpamClassifier {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new ApplySpamClassifierConf(argv)

    val conf = new SparkConf().setAppName("Spam Classifier")
    val sc = new SparkContext(conf)

    log.info("input : " + args.input())
    log.info("output : " + args.output())
    log.info("model : " + args.model())

    val model = sc.broadcast(sc.textFile(args.model())
      .map(line => {
        val items = line.substring(1,line.length-1).split(",")
        items(1).toInt -> items(2).toDouble
      }).collectAsMap())


    var results = sc.textFile(args.input())
      .map(line => {
        val items = line.split(" ")
      })




  }

}
