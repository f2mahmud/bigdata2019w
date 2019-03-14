package ca.uwaterloo.cs451.a5

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.mutable.ListBuffer

class Q5Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val text: ScallopOption[Boolean] = toggle("text", default = Some(false))
  val parquet: ScallopOption[Boolean] = toggle("parquet", default = Some(false))
  verify()
}

object Q5 {
  //TODO::Should double check how to display raw data
  //TODO::Verify answer using spark sql
  //TODO::Plot graph

  //3 is canada, 24 is us

  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new Q5Conf(argv)

    log.info("input : " + args.input)

    val conf = new SparkConf().setAppName("Q5")
    val sc = new SparkContext(conf)

    if (args.text.apply()) {

      log.info("type : text")

      val nations = sc.broadcast(sc.textFile(args.input() + "/nation.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          (lineArray(0), lineArray(1)) //key, name
        }).collectAsMap())

      val customers = sc.broadcast(sc.textFile(args.input() + "/customer.tbl")
        .flatMap(line => {
          val lineArray = line.split("\\|")
          if (lineArray(3).toInt == 3 || lineArray(3).toInt == 24) {
            List(lineArray(0) -> nations.value(lineArray(3))) //custkey -> nation name
          } else {
            List()
          }
        }).collectAsMap())

      val orders = sc.textFile(args.input() + "/orders.tbl")
        .flatMap(order => {
          val orderArray = order.split("\\|")
          if (customers.value.contains(orderArray(1))) {
            List((orderArray(0), customers.value(orderArray(1)))) //orderid, nation name
          } else List()
        })


      val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          (lineArray(0), lineArray(10).substring(0, 7)) //orderkey, date
        })


      lineItems.cogroup(orders) //orderkey, listofdates, nations
        .flatMap(item => {
        if (item._2._2.nonEmpty) {
          var l: ListBuffer[((String, String), Int)] = ListBuffer()
          item._2._1.foreach(date => {
            l += (((date, item._2._2.head), 1))
          })
          l.toList
        } else {
          List()
        }
      }).reduceByKey(_ + _)
        .sortBy(_._1, true, numPartitions = 1)
        .collect()
        .foreach(item => {
          println("(" + item._1._1 + "," + item._1._2 + "," + item._2 + ")")
        })


    } else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()

      val nations = sc.broadcast(sparkSession.read.parquet(args.input() + "/nation").rdd
        .map(line => {
          (line.getInt(0), line.getString(1))
        }).collectAsMap())

      val customers = sc.broadcast(sparkSession.read.parquet(args.input() + "/customer").rdd
        .flatMap(line => {
          if (line.getInt(3) == 3 || line.getInt(3) == 24) {
            List(line.getInt(0) -> nations.value(line.getInt(3)))
          } else {
            List()
          }
        }).collectAsMap())

      val orders = sparkSession.read.parquet(args.input() + "/orders").rdd
        .flatMap(order => {
          if (customers.value.contains(order.getInt(1))) {
            List((order.getInt(0), customers.value(order.getInt(1))))
          } else {
            List()
          }
        })

      val lineItems = sparkSession.read.parquet(args.input() + "/lineitem").rdd
        .map(item => {
          (item.getInt(0), item.getString(10).substring(0, 7))
        })

      lineItems.cogroup(orders)
        .flatMap(item => {
          if (item._2._2.nonEmpty) {
            var l: ListBuffer[((String, String), Int)] = ListBuffer()
            item._2._1.foreach(date => {
              l += (((date, item._2._2.head), 1))
            })
            l.toList
          } else {
            List()
          }
        }).reduceByKey(_ + _)
        .sortBy(_._1, true, numPartitions = 1)
        .collect()
        .foreach(item => {
          println("(" + item._1._1 + "," + item._1._2 + "," + item._2 + ")")
        })

    }

  }

}
