package ca.uwaterloo.cs451.a2

import io.bespin.scala.util.Tokenizer
import org.apache.log4j._
import org.apache.hadoop.fs._
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.rogach.scallop._
import scala.collection.mutable.MutableList


class ConfPairsBRF(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, output, reducers)
  val input: ScallopOption[String] =
    opt[String](descr = "input path", required = true)
  val output: ScallopOption[String] =
    opt[String](descr = "output path", required = true)
  val reducers: ScallopOption[Int] =
    opt[Int](descr = "number of reducers", required = false, default = Some(1))
  var threshold: ScallopOption[Int] = opt[Int](descr =
    "threshold for total count",
    required = false,
    default = Some(0))
  verify()
}

object ComputeBigramRelativeFrequencyPairs extends Tokenizer {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {
    val args = new ConfPairsBRF(argv)

    log.info("Input: " + args.input())
    log.info("Output: " + args.output())
    log.info("Number of reducers: " + args.reducers())

    val conf = new SparkConf().setAppName("Bigram Relative Frequency Pairs")
    //conf.set("spark.default.parallelism", args.reducers().toString)
    val sc = new SparkContext(conf)

    val outputDir = new Path(args.output())
    FileSystem.get(sc.hadoopConfiguration).delete(outputDir, true)

    val textFile = sc.textFile(args.input())

    var marginal: Float = 0

    val bigramCounts = textFile
      .flatMap(
        line => {
          val tokens = tokenize(line)
          if (tokens.length > 1) {
            var pairs: MutableList[((String, String), Float)] = MutableList()
            for (i <- 1 until tokens.length) {
              pairs += (((tokens(i - 1), tokens(i)), 1f))
              pairs += (((tokens(i - 1), "*"), 1f))
            }
            pairs
          }
          else List()
        }
      )
      .reduceByKey(_ + _, args.reducers())
      .sortByKey()
      .map({
        case (key, value) => {
          if (key._2 == "*") {
            marginal = value
            (key, value)
          }
          else {
            (key, value / marginal)
          }
        }
      })

    bigramCounts.saveAsTextFile(args.output())
  }

}