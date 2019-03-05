package ca.uwaterloo.cs451.a5

import org.apache.log4j.Logger
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

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)


    val date = args.date()

    if (args.text.apply()) {

      log.info("type : text")

      val nations = sc.textFile(args.input() + "/nation.tbl")
      val customers = sc.textFile(args.input() + "/customer.tbl")
      val order = sc.textFile(args.input() + "/order.tbl")

      //Getting top 20 orders on that day
      val lineItems: Array[List[Int]] = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap { case line => {
          val lineArray = line.split("\\|")
          if (lineArray(10).substring(0, date.length).equals(date)) {
            List(List(lineArray(0).toInt)) //orderkey
          } else {
            List()
          }
        }
        }.collect()

      val parts = sc.textFile(args.input() + "/part.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          lineArray(0).toInt -> lineArray(1) //key,name
        }).collectAsMap()

      val suppliers = sc.textFile(args.input() + "/supplier.tbl")
        .map(line => {
          val lineArray = line.split("\\|")
          (lineArray(0).toInt, lineArray(1)) //key, name
        }).collectAsMap()


      lineItems.foreach(lineItem => {
        val partName = parts(lineItem(1))
        val supplierName = suppliers(lineItem(2))
        println("(" + lineItem(0) + "," + partName + "," + supplierName + ")")
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

      lineItems.sortBy(item => item(0)).take(20)
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
      date + "' order by l_orderkey asc limit 20").show()

  }
}
