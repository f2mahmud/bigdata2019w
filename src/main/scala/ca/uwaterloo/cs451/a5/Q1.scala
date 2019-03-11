package ca.uwaterloo.cs451.a5


import org.apache.log4j.Logger
import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.rogach.scallop.{ScallopConf, ScallopOption}

//query : select count(*) from lineitem where l_shipdate = 'YYYY-MM-DD';
//done

class Q1Conf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, date)
  val input: ScallopOption[String] = opt[String](descr = "input path", required = true)
  val date: ScallopOption[String] = opt[String](descr = "output path", required = true)
  val text: ScallopOption[Boolean] = toggle("text", default = Some(false))
  val parquet: ScallopOption[Boolean] = toggle("parquet", default = Some(false))
  verify()
}

object Q1 {

  val log = Logger.getLogger(getClass().getName())


  def main(argv: Array[String]) {

    val args = new Q1Conf(argv)

    log.info("input : " + args.input)
    log.info("date : " + args.date)

    val conf = new SparkConf().setAppName("Pairs PMI")
    val sc = new SparkContext(conf)

    val date = args.date()
    var count = sc.longAccumulator

    if (args.text.apply()) {

      log.info("type : text")

      val textFile = sc.textFile(args.input() + "/lineitem.tbl")

      val items = textFile
        .foreach(line => {
          val dateFromRow = line.split("\\|")(10)
          if (dateFromRow.substring(0, date.length).equals(date)) {
            count.add(1)
          }
        })

    } else {

      log.info("type : parquet")

      val sparkSession = SparkSession.builder().getOrCreate()
      val textFileDF = sparkSession.read.parquet(args.input() + "/lineitem")
      val textFile = textFileDF.rdd

      val items = textFile
        .foreach(line => {
          val dateFromRow = line.getString(10)
          if (dateFromRow.substring(0, date.length).equals(date)) {
            count.add(1)
          }
        })

    }

    println("ANSWER=" + count.value)

    //TODO:REMOVE
    //    val parquet = "TPC-H-0.1-PARQUET"
    val parquet = "/data/cs451/TPC-H-10-PARQUET"


    val sqlContext = new SQLContext(sc)

    val lineitem = sqlContext.read.parquet(parquet + "/lineitem")
    val orders = sqlContext.read.parquet(parquet + "/orders")

    lineitem.registerTempTable("lineitem")
    orders.registerTempTable("orders")
    println("Given >>>>>>>>>> ")

    //TODO:: the ''s need to be there for date
    val sqlAns = sqlContext.sql("select count(*) from lineitem where l_shipdate = '" + date + "'").show()

  }


}
