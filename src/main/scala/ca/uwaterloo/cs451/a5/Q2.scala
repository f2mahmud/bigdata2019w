package ca.uwaterloo.cs451.a5

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}
import scala.collection.mutable.ListBuffer

/*
    select o_clerk, o_orderkey from lineitem, orders
      where
        l_orderkey = o_orderkey and
        l_shipdate = 'YYYY-MM-DD'
      order by o_orderkey asc
      limit 20;
  */

class Q2Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val date: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val text: ScallopOption[Boolean] = toggle("text", default = Some(false))
  val parquet: ScallopOption[Boolean] = toggle("parquet", default = Some(false))
  verify()
}

object Q2 {

  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {

    val args = new Q2Conf(argv)

    log.info("input : " + args.input)
    log.info("date : " + args.date)

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)


    val date: String = args.date()

    if (args.text.apply()) {

      log.info("type : text")

      //Getting top 20 orders on that day
      val lineItems: RDD[(Int, String)] = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val lineArray = line.split("\\|")
          if (lineArray(10).substring(0, date.length).equals(date)) {
            List((lineArray(0).toInt, lineArray(10))) //orderid, date
          } else {
            List()
          }
        }
        )

      val orders: RDD[(Int, String)] = sc.textFile(args.input() + "/orders.tbl")
        .map(order => {
          val orderArray = order.split("\\|")
          (orderArray(0).toInt, "(" + orderArray(6) + "," + orderArray(0) + ")") //orderid, clerk
        })

      var listBuffer: ListBuffer[String] = ListBuffer[String]()

      lineItems.cogroup(orders)
        .filter(_._2._1.nonEmpty)
        .sortBy(item => item._1, numPartitions = 1)
        .take(20)
        .foreach(item => {
          item._2._1.foreach(sub => {
            listBuffer += item._2._2.toList.head
          })
        })

      for (i <- 0 to 19) {
        println(listBuffer(i))
      }


    }
    else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()

      val lineItemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")

      val lineItemsRDD = lineItemDF.rdd
        .flatMap(line => {
          val dateFromRow = line.getString(10)
          if (dateFromRow.substring(0, date.length).equals(date)) {
            List((line.getInt(0), dateFromRow)) //orderid, date
          } else {
            List()
          }
        })

      val ordersRDD = ordersDF.rdd
        .map(item => {
          (item.getInt(0), "(" + item.get(6) + "," + item.get(0) + ")")
        })

      var listBuffer: ListBuffer[String] = ListBuffer[String]()

      lineItemsRDD.cogroup(ordersRDD)
        .filter(_._2._1.nonEmpty)
        .sortBy(item => item._1, numPartitions = 1) //TODO::check if sortByKey worls
        .take(20)
        .foreach(item => {
          item._2._1.foreach(sub => {
            listBuffer += item._2._2.toList.head
          })
        })

      for (i <- 0 to 19) {
        println(listBuffer(i))
      }


    }

  }

}
