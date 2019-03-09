package ca.uwaterloo.cs451.a5

import org.apache.log4j.Logger
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.rogach.scallop.{ScallopConf, ScallopOption}

class Q6Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val date: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val text: ScallopOption[Boolean] = toggle("text", default = Some(false))
  val parquet: ScallopOption[Boolean] = toggle("parquet", default = Some(false))
  verify()
}

/*
  select
    l_returnflag,
    l_linestatus,
    sum(l_quantity) as sum_qty,
    sum(l_extendedprice) as sum_base_price,
    sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
    sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
    avg(l_quantity) as avg_qty,
    avg(l_extendedprice) as avg_price,
    avg(l_discount) as avg_disc,
    count(*) as count_order
  from lineitem
  where
  l_shipdate = 'YYYY-MM-DD'
group by l_returnflag, l_linestatus;
 */
object Q6 {


  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]) {

    val args = new Q6Conf(argv)

    log.info("input : " + args.input)
    log.info("date : " + args.date)

    val conf = new SparkConf().setAppName("Q6")
    val sc = new SparkContext(conf)


    val date = args.date()

    if (args.text.apply()) {

      log.info("type : text")


      val lineItems = sc.textFile(args.input() + "/lineitem.tbl")
        .flatMap(line => {
          val lineArray = line.split("\\|")
          if (lineArray(10).substring(0, date.length).equals(date)) {
            var discountPrice: Float = lineArray(5).toFloat * (1f - lineArray(6).toFloat)

            List(((lineArray(8), lineArray(9)),
              (lineArray(4).toFloat, lineArray(5).toFloat, discountPrice,
                discountPrice * (1f + lineArray(7).toFloat), lineArray(6).toFloat, 1f)))

          } else {
            List()
          }
        })
        .reduceByKey((accum, item) => {
          (accum._1 + item._1, accum._2 + item._2, accum._3 + item._3, accum._4 + item._4, accum._5 + item._5, accum._6 + item._6)
        }).map(item => {
        val sub = item._2
        (item._1._1, item._1._2, sub._1, sub._2, sub._3, sub._4, sub._1 / sub._6, sub._2 / sub._6, sub._5 / sub._6, sub._6)
      }).foreach(println(_))


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

    lineitem.registerTempTable("lineitem")
    println("Given >>>>>>>>>> ")

    //TODO:: the ''s need to be there for date
    val sqlAns = sqlContext.sql("select  l_returnflag,  l_linestatus,  sum(l_quantity) as sum_qty," +
      " sum(l_extendedprice) as sum_base_price,  sum(l_extendedprice*(1-l_discount)) as sum_disc_price," +
      "sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge, avg(l_quantity) as avg_qty," +
      " avg(l_extendedprice) as avg_price,  avg(l_discount) as avg_disc,  count(*) as count_order " +
      "from lineitem\nwhere\n  l_shipdate = '" +
      date + "' group by l_returnflag, l_linestatus").show(200)

  }
}
