package ca.uwaterloo.cs451.a6

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class ApplyEnsembleSpamClassifierConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, model)
  val input: ScallopOption[String] = opt[String](descr = "input test instances", required = true)
  val output: ScallopOption[String] = opt[String](descr = "output path", required =  true)
  val model: ScallopOption[String] = opt[String](descr = "classifier model", required = true)
  verify()
}

object ApplyEnsmbleSpamClassifier {

}
