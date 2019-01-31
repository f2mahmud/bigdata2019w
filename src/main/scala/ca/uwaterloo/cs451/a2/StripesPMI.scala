package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.mutable.Map

class ConfPMIStripes(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val reducers: ScallopOption[Int] = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  var threshold: ScallopOption[Float] = opt[Float](descr = "threshold for total count", required = false, default = Some(0f))
  verify()
}

object StripesPMI extends Tokenizer {
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
    val args = new ConfPMIStripes(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Stripes PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())
    val numberOfLines = textFile.count().toFloat
    val broadcastLineCount = sc.broadcast(numberOfLines)

    val wordOccurences = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val occurenceMap: Map[String, Float] = Map()
        if (tokens.length > 1) {
          for (index <- 0 to Math.min(40, tokens.length - 1)) {
            occurenceMap += tokens(index) -> 1f
          }
          List(occurenceMap)
        } else List()
      })
      .flatMap((map) => map)
      .reduceByKey(_ + _, args.reducers())
      .map(item => (item._1, item._2 / broadcastLineCount.value))

    val broadCastedwordCount = sc.broadcast(wordOccurences.collectAsMap())
    val broadcastedThreshold = sc.broadcast(args.threshold())

    val occurenceCounts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        var stripes: Map[String, Map[String, Float]] = Map()
        if (tokens.length > 1) {
          for (i <- 0 until Math.min(40, tokens.length)) {
            var partners: Map[String, Float] = Map()
            for (j <- 0 until Math.min(40, tokens.length)) {
              if (tokens(i) != tokens(j)) {
                partners += tokens(j) -> 1f
              }
            }
            stripes += tokens(i) -> partners
          }
          List(stripes)
        } else List()
      })
      .flatMap(insideMap => insideMap)
      .reduceByKey((accum, n) => reduceMaps(accum, n), args.reducers())
      .map((item) => {
        item._2.foreach((subItem) => {
          if (subItem._2 > broadcastedThreshold.value) {
            item._2 += subItem._1 ->
              Math.log10(
                (subItem._2 / broadcastLineCount.value)
                  / (broadCastedwordCount.value(item._1) * broadCastedwordCount.value(subItem._1))).toFloat
            subItem._2
          } else item._2.remove(subItem._1)
        })
        item
      })
    occurenceCounts.saveAsTextFile(args.output())
  }

}

