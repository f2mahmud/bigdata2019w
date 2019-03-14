package ca.uwaterloo.cs451.a5

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

/*
    select l_orderkey, p_name, s_name from lineitem, part, supplier
      where
        l_partkey = p_partkey and
        l_suppkey = s_suppkey and
        l_shipdate = 'YYYY-MM-DD'
    order by l_orderkey asc limit 20;
*/

//TODO::need to implement hash join

class Q3Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val date: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val text: ScallopOption[Boolean] = toggle("text", default = Some(false))
  val parquet: ScallopOption[Boolean] = toggle("parquet", default = Some(false))
  verify()
}

object Q3 {

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new Q3Conf(argv)

    log.info("input : " + args.input)
    log.info("date : " + args.date)

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)


    val date = args.date()

    if (args.text.apply()) {

      log.info("type : text")

      val parts = sc.broadcast(sc.textFile(args.input() + "/part.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          lineArray(0) -> lineArray(1) //key,name
        }).collectAsMap())

      val suppliers = sc.broadcast(sc.textFile(args.input() + "/supplier.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          lineArray(0) -> lineArray(1) //key, name
        }).collectAsMap())

      //Getting top 20 orders on that day
      val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val lineArray = line.split("\\|")
          if (lineArray(10).substring(0, date.length).equals(date)) {
            List((lineArray(0).toInt, parts.value(lineArray(1)), suppliers.value(lineArray(2)))) //orderkey, partkey, supkey
          } else {
            List()
          }
        })
        .sortBy(_._1, true, numPartitions = 1)
        .take(20)
        .toList
        .foreach {
          case (order, part, supplier) => println("(" + order + "," + part + "," + supplier + ")")
        }

      parts.unpersist()
      parts.destroy()

      suppliers.unpersist()
      suppliers.destroy()


    } else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()

      val lineItemDF = sparkSession.read.parquet(args.input() + "/lineitem").rdd
      val partsDF = sparkSession.read.parquet(args.input() + "/part").rdd
      val suppliersDF = sparkSession.read.parquet(args.input() + "/supplier").rdd


      val parts = sc.broadcast(partsDF.map(line => line.getInt(0) -> line.getString(1)).collectAsMap())

      val suppliers = sc.broadcast(suppliersDF.map(line => line.getInt(0) -> line.getString(1)).collectAsMap())


      val lineItems = lineItemDF
        .flatMap(line => {
          val dateFromRow = line.getString(10)
          if (dateFromRow.substring(0, date.length).equals(date)) {
            List((line.getInt(0), parts.value(line.getInt(1)), suppliers.value(line.getInt(2))))
          } else {
            List()
          }
        })
        .sortBy(_._1, true, numPartitions = 1)
        .take(20)
        .toList
        .foreach(println(_))      //TODO:: might not print properly

    }

  }

}
