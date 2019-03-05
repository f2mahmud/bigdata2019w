package ca.uwaterloo.cs451.a5

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

import scala.collection.mutable.ListBuffer

/*
    select l_orderkey, p_name, s_name from lineitem, part, supplier
      where
        l_partkey = p_partkey and
        l_suppkey = s_suppkey and
        l_shipdate = 'YYYY-MM-DD'
    order by l_orderkey asc limit 20;
*/

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

  def isContained(list: List[ListBuffer[String]], item: String): Int = {
    for ((key, value) <- n) {
      if (accum.contains(key)) {
        accum += key -> (accum(key) + value)
      } else accum += key -> value
    }
    accum
  }


  def main(argv: Array[String]) {

    val args = new Q2Conf(argv)

    log.info("input : " + args.input)
    log.info("date : " + args.date)

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)


    val date = args.date()

    if (args.text.apply()) {

      log.info("type : text")

      val parts = sc.textFile(args.input() + "/part.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          (lineArray(0), lineArray(1)) //key,name
        }).collectAsMap()

      val suppliers = sc.textFile(args.input() + "/supplier.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          (lineArray(0), lineArray(1)) //key, name
        }).collectAsMap()

      //Getting top 20 orders on that day
      val lineItems: Array[ListBuffer[String]] = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap { case line => {
          val lineArray = line.split("\\|")
          if (lineArray(10).substring(0, date.length).equals(date)) {
            List(ListBuffer(lineArray(0), lineArray(1), lineArray(2))) //orderkey, partkey, supkey
          } else {
            List()
          }
        }
        }.sortBy(_ (0).toInt, true).take(20)

      lineItems.foreach(lineitem => {
        val partName = parts.get(lineitem(1))
        val supplierName = suppliers.get(lineitem(2))
        println("(" + lineItems(0) + "," + partName + "," + supplierName + ")")
      })


    } else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()

      val lineItemDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val ordersDF = sparkSession.read.parquet(args.input() + "/orders")

      val lineItemsRDD = lineItemDF.rdd
      val ordersRDD = ordersDF.rdd

      val filteredLineItems = lineItemsRDD
        .map(line => {
          val dateFromRow = line.getString(10)
          if (dateFromRow.substring(0, date.length).equals(date)) {
            line.get(0)
          }
        })

      val orders = ordersRDD.foreach(line => {
        filteredLineItems.collect().foreach(lineItem => {
          if (lineItem.equals(line.get(0))) {
            println("(" + line(6) + "," + line(0) + ")")
          }
        })
      })


    }


    //TODO:REMOVE
    val parquet = "TPC-H-0.1-PARQUET"


    val sqlContext = new SQLContext(sc)

    val lineitem = sqlContext.read.parquet(parquet + "/lineitem")
    val part = sqlContext.read.parquet(parquet + "/part")
    val supplier = sqlContext.read.parquet(parquet + "/supplier")

    lineitem.registerTempTable("lineitem")
    part.registerTempTable("part")
    supplier.registerTempTable("supplier")
    println("Given >>>>>>>>>> ")

    //TODO:: the ''s need to be there for date
    val sqlAns = sqlContext.sql("select l_orderkey, p_name, s_name from lineitem, part," +
      " supplier where l_partkey = p_partkey and l_suppkey = s_suppkey and " +
      "l_shipdate = '" +
      date + "' order by o_orderkey asc limit 20").show()

  }
}
