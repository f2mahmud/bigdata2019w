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

      //TEXT DONE:: decimal points might be an issue
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
        .take(10)
        .foreach(item => {
          println("(" + item._1._1 + "," + item._1._2 + "," + item._2 + "," + item._1._3 + "," + item._1._4 + ")")
        })


    }

    else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()

      val orders : RDD[(String,(String,String,String))] = sparkSession.read.parquet(args.input() + "/orders").rdd
        .flatMap(order => {
          if(LocalDate.parse(order.getString(4)).isBefore(date)){
            List( (order.getString(0),(order.getString(1), order.getString(4), order.getString(7))))
          }else{
            List()
          }
        })

      val lineItems = sparkSession.read.parquet(args.input() + "/lineitem").rdd
        .flatMap(line => {
          if(LocalDate.parse(line.getString(10)).isAfter(date)){
            List((line.getString(0), line.getFloat(5) * (1f - line.getFloat(6))))
          }else{
            List()
          }
        })

      val customers = sc.broadcast(sparkSession.read.parquet(args.input() + "/customer").rdd
          .map(customer => {
            customer.getString(0) -> customer.getString(1)
          }).collectAsMap()
      )

      lineItems.cogroup(orders)
        .flatMap(item => {
          if (item._2._1.nonEmpty && item._2._2.nonEmpty) {
            val l: ListBuffer[((String, String, String, String), Float)] = ListBuffer()
            val key: (String,String,String,String) = (customers.value(item._2._2.head._1), item._1, item._2._2.head._2, item._2._2.head._3)
            item._2._1.foreach(sub => {
              l += ((key, sub))
            })
            l.toList
          } else {
            List()
          }
        }).reduceByKey(_ + _)
        .sortBy(_._2, false, 1)
        .take(10)
        .foreach(item => {
          println("(" + item._1._1 + "," + item._1._2 + "," + item._2 + "," + item._1._3 + "," + item._1._4 + ")")
        })


    }


    //TODO:REMOVE
    //    val parquet = "TPC-H-0.1-PARQUET"
    val parquet = "/data/cs451/TPC-H-10-PARQUET"


    val sqlContext = new SQLContext(sc)

    val lineitem = sqlContext.read.parquet(parquet + "/lineitem")
    val customer = sqlContext.read.parquet(parquet + "/customer")
    val orders = sqlContext.read.parquet(parquet + "/orders")

    lineitem.registerTempTable("lineitem")
    customer.registerTempTable("customer")
    orders.registerTempTable("orders")
    println("Given >>>>>>>>>> ")

    //TODO:: the ''s need to be there for date
    val sqlAns = sqlContext.sql("select  c_name,  l_orderkey,  sum(l_extendedprice*(1-l_discount)) as revenue," +
      " o_orderdate, o_shippriority from customer, orders, lineitem " +
      "where c_custkey = o_custkey and l_orderkey = o_orderkey and o_orderdate < '" + date + "' and " +
      "l_shipdate > '" + date + "' group by c_name, l_orderkey, o_orderdate, o_shippriority order by revenue desc limit 10")
      .show(200)

  }

}
