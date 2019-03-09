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
        .flatMap(order => {
          val orderArray = order.split("\\|")
          List((orderArray(0), orderArray(1))) //orderid, custkey
        })


      val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap { case line => {
          val lineArray = line.split("\\|")
          if (lineArray(10).substring(0, date.length).equals(date)) {
            List((lineArray(0), lineArray(10))) //orderkey, random
          } else {
            List()
          }
        }
        }

      lineItems.cogroup(orders)
        .filter(_._2._1.toList.nonEmpty)
        .map(item => {
          (customers.value(item._2._2.head), item._2._1.size)
        })
        .reduceByKey(_ + _)
        .sortBy(_._1, true, numPartitions = 1)
        .foreach(item => {
          println("(" + item._1._1 + "," + item._1._2 + "," + item._2 + ")")
        })


    } else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()

      val lineItemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val partsDF = sparkSession.read.parquet(args.input() + "/part")
      val suppliersDF = sparkSession.read.parquet(args.input() + "/supplier")

      val lineItemsRDD = lineItemDF.rdd
      val partsRDD = partsDF.rdd
      val suppliersRDD = suppliersDF.rdd

      val parts = partsRDD.map(part => {
        (part(0), part(1))
      }).collectAsMap()

      val suppliers = suppliersRDD.map(supplier => {
        (supplier(0), supplier(1))
      }).collectAsMap()

      val lineItems = lineItemsRDD.flatMap(line => {
        val dateFromRow = line.getString(10)
        if (dateFromRow.substring(0, date.length).equals(date)) {
          List(List(line.getInt(0), line.getInt(1), line.getInt(2)))
        } else {
          List()
        }
      })

      lineItems.sortBy(item => item(0), numPartitions = 1).take(20)
        .foreach(item => {
          val partName = parts(item(1))
          val supplierName = suppliers(item(2))
          println("(" + item(0) + "," + partName + "," + supplierName + ")")
        })

    }


    //TODO:REMOVE
    val parquet = "TPC-H-0.1-PARQUET"


    val sqlContext = new SQLContext(sc)

    val lineitem = sqlContext.read.parquet(parquet + "/lineitem")
    val order = sqlContext.read.parquet(parquet + "/orders")
    val customer = sqlContext.read.parquet(parquet + "/customer")
    val nation = sqlContext.read.parquet(parquet + "/nation")

    lineitem.registerTempTable("lineitem")
    order.registerTempTable("orders")
    customer.registerTempTable("customer")
    nation.registerTempTable("nation")
    println("Given >>>>>>>>>> ")

    //TODO:: the ''s need to be there for date
    val sqlAns = sqlContext.sql("select n_nationkey, n_name, count(*) from lineitem, orders, customer, nation " +
      "where  l_orderkey = o_orderkey and  o_custkey = c_custkey and  c_nationkey = n_nationkey and  l_shipdate = '" +
      date + "'group by n_nationkey, n_name order by n_nationkey asc").show(50)

  }
}
