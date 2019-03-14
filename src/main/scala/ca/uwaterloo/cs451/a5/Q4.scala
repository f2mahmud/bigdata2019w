package ca.uwaterloo.cs451.a5

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class Q4Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val date: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val text: ScallopOption[Boolean] = toggle("text", default = Some(false))
  val parquet: ScallopOption[Boolean] = toggle("parquet", default = Some(false))
  verify()
}

object Q4 {


  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new Q4Conf(argv)

    log.info("input : " + args.input)
    log.info("date : " + args.date)

    val conf = new SparkConf().setAppName("Q4")
    val sc = new SparkContext(conf)


    val date = args.date()

    if (args.text.apply()) {
      //DONE Text
      log.info("type : text")

      val nations = sc.broadcast(sc.textFile(args.input() + "/nation.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          (lineArray(0), lineArray(1)) //key, name
        }).collectAsMap())

      val customers = sc.broadcast(sc.textFile(args.input() + "/customer.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          lineArray(0) -> (lineArray(3).toInt, nations.value(lineArray(3))) //key -> nationkey, nation name
        }).collectAsMap())

      val orders: RDD[(String, String)] = sc.textFile(args.input() + "/orders.tbl")
        .map(order => {
          val orderArray = order.split("\\|")
          (orderArray(0), orderArray(1)) //orderid, custkey
        })


      val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val lineArray = line.split("\\|")
          if (lineArray(10).substring(0, date.length).equals(date)) {
            List((lineArray(0), lineArray(10))) //orderkey, random
          } else {
            List()
          }
        })

      lineItems.cogroup(orders)
        .flatMap(item => {
          if (item._2._1.nonEmpty) {
            List((customers.value(item._2._2.head), item._2._1.size))
          } else {
            List()
          }
        })
        .reduceByKey(_ + _)
        .sortBy(_._1, true, numPartitions = 1)
        .collect()
        .foreach(item => {
          println("(" + item._1._1 + "," + item._1._2 + "," + item._2 + ")")
        })


    } else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()

      val nations = sc.broadcast(sparkSession
        .read.parquet(args.input() + "/nation").rdd
        .map(line => (line.getInt(0), line.getString(1))).collectAsMap())

      val customers = sc.broadcast(sparkSession
        .read.parquet(args.input() + "/customer").rdd
        .map(line => line.getInt(0) -> (line.getInt(3), nations.value(line.getInt(3))))
        .collectAsMap())

      val orders : RDD[(Int,Int)] = sparkSession.read.parquet(args.input() + "/orders").rdd
        .map(item => (item.getInt(0), item.getInt(1)))

      val lineItem = sparkSession.read.parquet(args.input() + "/lineitem").rdd
        .flatMap(line => {
          val dateFromRow = line.getString(10)
          if (dateFromRow.substring(0, date.length).equals(date)) {
            List((line.getInt(0), dateFromRow))
          } else {
            List()
          }
        })

      lineItem.cogroup(orders)
        .flatMap(item => {
          if (item._2._1.nonEmpty) {
            List((customers.value(item._2._2.head), item._2._1.size))
          } else {
            List()
          }
        })
        .reduceByKey(_ + _)
        .sortBy(_._1, true, numPartitions = 1)
        .collect()
        .foreach(item => {
          println("(" + item._1._1 + "," + item._1._2 + "," + item._2 + ")")
        })

    }

  }

}
