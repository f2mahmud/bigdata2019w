package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ConfPairsPMI(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val reducers: ScallopOption[Int] = opt[Int](descr = "number of reducers", required = false, default = Some(1))
  var threshold: ScallopOption[Int] = opt[Int](descr = "threshold for total count", required = false, default = Some(0))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairsPMI(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())
    log.info("Number of reducers: " + args.threshold())

    val conf = new SparkConf().setAppName("Bigram Count")
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    val wordCount = textFile
      .flatMap(line => line.map(token => (token, 1)).distinct).reduceByKey(_ + _)
    val numberOfLines = textFile.count()

    val OccurenceCounts = textFile
      .flatMap(line => {
        val tokens = tokenize(line)
        if (tokens.length > 1)
          tokens.flatMap(line => line.map(
            token => token -> line.collect {
              case (innerToken) if (innerToken != token) => (innerToken, 1f)
            }))
        else List()
      })
      .map(bigram => (bigram, 1f))
      .reduceByKey(_ + _)


    OccurenceCounts.saveAsTextFile(args.output())
  }

}
