package ca.uwaterloo.cs451.a5

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

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
        .flatMap { case line => {
          val lineArray = line.split("\\|")
          List((lineArray(0).toInt, lineArray(10)))
        }
        }

      val orders: RDD[(Int, String)] = sc.textFile(args.input() + "/orders.tbl")
        .flatMap(order => {
          val orderArray = order.split("\\|")
          List((orderArray(0).toInt, orderArray(6)))
        })

      val results = lineItems.cogroup(orders)
        .filter { case item => item._2._1.toList(0).substring(0, date.length).equals(date) }
        .sortBy(item => item._1, numPartitions = 1)
        .take(20)
        .map(filteredItems => {
          (filteredItems._2._2.toList(0), filteredItems._1)
        })
        .foreach(item => {
          println("(" + item._1 + "," + item._2 + ")")
        })
      //        .foreach(item => {
      //          )
      //          {
      //
      //          }
      //          else
      //          {
      //            List()
      //          }
      //          println("(" + item._2._2.toList(0) + "," + item._1 + ")")
      //        })
      //        .foreach(line => {
      //          val lineArray = line.split("\\|")
      //          lineItems.foreach(lineItem => {
      //            if (lineItem.equals(lineArray(0))) {
      //              println("(" + lineArray(6) + "," + lineArray(0) + ")")
      //            }
      //          })
      //        })

    } else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()

      val lineItemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")

      val lineItemsRDD = lineItemDF.rdd
      val ordersRDD = ordersDF.rdd

      val filteredLineItems: Array[Int] = lineItemsRDD
        .flatMap(line => {
          val dateFromRow = line.getString(10)
          if (dateFromRow.substring(0, date.length).equals(date)) {
            List(line.getInt(0))
          } else {
            List()
          }
        }).sortBy(item => item).take(20)

      val orders = ordersRDD.foreach(line => {
        filteredLineItems.foreach(lineItem => {
          if (lineItem.equals(line.getInt(0))) {
            println("(" + line(6) + "," + line(0) + ")")
          }
        })
      })


    }

    //TODO:REMOVE
    val parquet = "TPC-H-0.1-PARQUET"


    val sqlContext = new SQLContext(sc)

    val lineitem = sqlContext.read.parquet(parquet + "/lineitem")
    val orders = sqlContext.read.parquet(parquet + "/orders")

    lineitem.registerTempTable("lineitem")
    orders.registerTempTable("orders")
    println("Given >>>>>>>>>> ")

    //TODO:: the ''s need to be there for date
    val sqlAns = sqlContext.sql("select o_clerk, o_orderkey from lineitem, orders where l_orderkey = o_orderkey and l_shipdate = '" +
      date + "' order by o_orderkey asc limit 20").show()


  }
}
