package ca.uwaterloo.cs451.a5

import java.time.LocalDate

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.mutable.ListBuffer

class Q7Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val date: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val text: ScallopOption[Boolean] = toggle("text", default = Some(false))
  val parquet: ScallopOption[Boolean] = toggle("parquet", default = Some(false))
  verify()
}

/*
  select
  c_name,
  l_orderkey,
  sum(l_extendedprice*(1-l_discount)) as revenue,
  o_orderdate,
  o_shippriority
from customer, orders, lineitem
where
  c_custkey = o_custkey and
  l_orderkey = o_orderkey and
  o_orderdate < "YYYY-MM-DD" and
  l_shipdate > "YYYY-MM-DD"
group by
  c_name,
  l_orderkey,
  o_orderdate,
  o_shippriority
order by
  revenue desc
limit 10;
 */
object Q7 {


  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new Q7Conf(argv)

    log.info("input : " + args.input)
    log.info("date : " + args.date)

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)


    val date = LocalDate.parse(args.date())

    if (args.text.apply()) {

      log.info("type : text")


      val orders: RDD[(String, (String, String, String))] = sc.textFile(args.input() + "/orders.tbl")
        .flatMap(order => {
          val orderArray = order.split("\\|")
          if (LocalDate.parse(orderArray(4)).isBefore(date)) {
            List((orderArray(0), (orderArray(1), orderArray(4), orderArray(7)))) //orderid -> customerid, orderdate, ship-priority
          }
          else {
            List()
          }
        })


      val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val lineArray = line.split("\\|")
          if (LocalDate.parse(lineArray(10)).isAfter(date)) {
            List((lineArray(0), lineArray(5).toFloat * (1f - lineArray(6).toFloat)))
          } else {
            List()
          }
        })

      val customers = sc.broadcast(sc.textFile(args.input() + "/customer.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          lineArray(0) -> lineArray(1) //key -> customer name
        }).collectAsMap())

      lineItems.cogroup(orders)
        .flatMap(item => {
          if (item._2._1.nonEmpty && item._2._2.nonEmpty) {
            val l: ListBuffer[((String, String, String, String), Float)] = ListBuffer()
            val key = (customers.value(item._2._2.head._1), item._1, item._2._2.head._2, item._2._2.head._3)
            item._2._1.foreach(sub => {
              l += ((key, sub))
            })
            l.toList
          } else {
            List()
          }
        }).reduceByKey(_ + _)
        .sortBy(_._2, false, 1)
        //.take(10)
        .foreach(item => {
          println("(" + item._1._1 + "," + item._1._2 + "," + item._2 + "," + item._1._3 + "," + item._1._4 + ")")
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

      //      val lineItems = lineItemsRDD.flatMap(line => {
      //        val dateFromRow = line.getString(10)
      //        if (dateFromRow.substring(0, date.length).equals(date)) {
      //          List(List(line.getInt(0), line.getInt(1), line.getInt(2)))
      //        } else {
      //          List()
      //        }
      //      })
      //
      //      lineItems.sortBy(item => item(0), numPartitions = 1).take(20)
      //        .foreach(item => {
      //          val partName = parts(item(1))
      //          val supplierName = suppliers(item(2))
      //          println("(" + item(0) + "," + partName + "," + supplierName + ")")
      //        })

    }


    //TODO:REMOVE
    val parquet = "TPC-H-0.1-PARQUET"


    val sqlContext = new SQLContext(sc)

    val lineitem = sqlContext.read.parquet(parquet + "/lineitem")

    lineitem.registerTempTable("lineitem")
    println("Given >>>>>>>>>> ")

    //TODO:: the ''s need to be there for date
    val sqlAns = sqlContext.sql("select  c_name,  l_orderkey,  sum(l_extendedprice*(1-l_discount)) as revenue," +
      " o_orderdate, o_shippriority from customer, orders, lineitem " +
      "where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '" + date + "' and " +
      "l_shipdate > '" + date + "' group by c_name, l_orderkey, o_orderdate, o_shippriority order by revenue desc limit 10;")
      .show(200)

  }
}
