package ca.uwaterloo.cs451.a2


import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.rogach.scallop._
import scala.collection.mutable.Map

class ConfStripesBRF(args: Seq[String]) extends ScallopConf(args) {

  mainOptions = Seq(input, output, reducers)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val reducers: ScallopOption[Int] = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  var threshold: ScallopOption[Int] = opt[Int](descr = "threshold for total count", required = false, default = Some(0))
  verify()
}

object ComputeBigramRelativeFrequencyStripes extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def reduceMaps(accum: Map[String, Float], n: Map[String, Float]): Map[String, Float] = {
    for ((key, value) <- n) {
      if (accum.contains(key)) {
        accum += key -> (accum(key) + value)
      } else accum += key -> value
    }
    accum
  }

  def main(argv: Array[String]) {
    val args = new ConfStripesBRF(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of reducers: " + args.threshold())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val counts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        var stripes: Map[String, Map[String, Float]] = Map()
        if (tokens.length > 1) {
          for (i <- 1 to tokens.length) {
            val previous = tokens(i - 1)
            val current = tokens(i)
            if (stripes.contains(previous)) {
              var stripe: Map[String, Float] = stripes.get(previous).get
              if (stripe.contains(current)) {
                stripe += current -> (stripe.get(current).get + 1f)
              } else stripe += current -> 1f
              stripes += previous -> stripe
            } else {
              var stripe: Map[String, Float] = Map()
              stripe += current -> 1f
              stripes += previous -> stripe
            }
          }
          List(stripes)
        }
        else List()
      })
      .flatMap(insideMap => insideMap)
      .reduceByKey((annum, n) => reduceMaps(annum, n), args.reducers())
      .map((items: (String, Map[String, Float])) => {
        var totalOccurences = 0
        items._2.foreach((item: (String, Float)) => totalOccurences += item._2)
        items._2.foreach((item: (String, Float)) => items._2 += item._1 -> (item._2/totalOccurences))
      })

    counts.saveAsTextFile(args.output())
  }
}