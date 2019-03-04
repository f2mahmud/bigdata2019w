package ca.uwaterloo.cs451.a5

import org.apache.log4j.Logger
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


    val date = args.date()

    if (args.text.apply()) {

      log.info("type : text")

      //Getting all the orders on that day
      val lineItems: Array[Any] = sc.textFile(args.input() + "/lineitem.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          if (lineArray(10).substring(0, date.length).equals(date)) {
            lineArray(0)
          }
        }).sortBy(_.toString, false).take(20)

      val orders = sc.textFile(args.input() + "/orders.tbl")
        .foreach(line => {
          val lineArray = line.split("\\|")
          lineItems.foreach(lineItem => {
            if (lineItem.equals(lineArray(0))) {
              println("(" + lineArray(6) + "," + lineArray(0) + ")")
            }
          })
        })

    } else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()

      val lineItemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")

      val lineItemsRDD = lineItemDF.rdd
      val ordersRDD = ordersDF.rdd

      val filteredLineItems: Array[Any] = lineItemsRDD
        .map(line => {
          val dateFromRow = line.getString(10)
          if (dateFromRow.substring(0, date.length).equals(date)) {
            line.get(0)
          }
        }).sortBy(_.toString, false).take(20)

      val orders = ordersRDD.foreach(line => {
        filteredLineItems.foreach(lineItem => {
          if (lineItem.equals(line.get(0))) {
            println("(" + line(6) + "," + line(0) + ")")
          }
        })
      })


    }

    val parquet = "TPC-H-0.1-PARQUET"

    //TODO:REMOVE
    val sqlContext = new SQLContext(sc)

    val lineitem = sqlContext.read.parquet(parquet + "/lineitem")
    val orders = sqlContext.read.parquet(parquet + "/orders")
    orders.printSchema()
    lineitem.registerTempTable("lineitem")
    orders.registerTempTable("orders")
    val sqlAns = sqlContext.sql("select o_clerk, o_orderkey from lineitem, orders where l_orderkey = o_orderkey and l_shipdate = " +
      date + " order by o_orderkey asc limit 20")

    sqlAns.foreach(line => {
      println("Given >>>>>>>>>> " + line)
    })

  }
}
