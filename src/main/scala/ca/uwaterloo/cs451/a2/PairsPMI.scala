package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.mutable.Map

class ConfPairsPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val reducers: ScallopOption[Int] = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  var threshold: ScallopOption[Int] = opt[Int](descr = "threshold for total count", required = false, default = Some(0))
  verify()
}

object PairsPMI extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairsPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of threshold: " + args.threshold())

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val numberOfLines = textFile.count()
    sc.broadcast(numberOfLines)

    val wordOccurences = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val occurenceMap: Map[(String, String), Float] = Map()
        if (tokens.length > 1) {
          for (index <- 0 to Math.min(40, tokens.length - 1)) {
            occurenceMap += (tokens(index), "*") -> 1f
          }
          List(occurenceMap)
        } else List()
      })
      .flatMap((map) => map)
      .reduceByKey(_ + _, args.reducers())

    val wordCount = sc.broadcast(wordOccurences.collectAsMap())

    val OccurenceCounts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        val occurenceMap: Map[(String, String), Float] = Map()
        if (tokens.length > 1) {
          for (index <- 0 to Math.min(40, tokens.length - 1)) {
            occurenceMap += (tokens(index), "*") -> 1f
            for (jindex <- 0 to Math.min(40, tokens.length - 1)) {
              if (tokens(index) != tokens(jindex)) {
                occurenceMap += (tokens(index), tokens(jindex)) -> 1f
              }
            }
          }
          List(occurenceMap)
        } else List()
      })
      .flatMap((map) => map)
      .reduceByKey(_ + _, args.reducers())


    wordOccurences.saveAsTextFile(args.output())
  }

  //TODO: does CountBigrams = CountWord-1

}
