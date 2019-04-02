package ca.uwaterloo.cs451.a7

/**
  * Bespin: reference implementations of "big data" algorithms
  *
  * Licensed under the Apache License, Version 2.0 (the "License");
  * you may not use this file except in compliance with the License.
  * You may obtain a copy of the License at
  *
  * http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */


import java.io.File
import java.util.concurrent.atomic.AtomicInteger

import org.apache.log4j._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming._
import org.apache.spark.streaming.scheduler.{StreamingListener, StreamingListenerBatchCompleted}
import org.rogach.scallop._

import scala.collection.mutable

class TrendingArrivalsConf(args: Seq[String]) extends ScallopConf(args) {
  mainOptions = Seq(input, checkpoint, output)
  val input = opt[String](descr = "input path", required = true)
  val checkpoint = opt[String](descr = "checkpoint path", required = true)
  val output = opt[String](descr = "output path", required = true)
  verify()
}

object TrendingArrivals {
  val log = Logger.getLogger(getClass().getName())

  def main(argv: Array[String]): Unit = {
    val args = new TrendingArrivalsConf(argv)

    log.info("Input: " + args.input())

    val spark = SparkSession
      .builder()
      .config("spark.streaming.clock", "org.apache.spark.util.ManualClock")
      .appName("EventCount")
      .getOrCreate()

    val numCompletedRDDs = spark.sparkContext.longAccumulator("number of completed RDDs")

    val batchDuration = Minutes(1)
    val ssc = new StreamingContext(spark.sparkContext, batchDuration)
    val batchListener = new StreamingContextBatchCompletionListener(ssc, 24)
    ssc.addStreamingListener(batchListener)

    def stateUpdateFunction(time: Time, key: String, newData: Option[Int], state: State[Int]): Option[(String, (Int, Long, Int))] = {
      
      var s = 0
      if (state.exists()) {
        s = state.get()
        if (newData.getOrElse(0) > 10 && s != 0 && Math.floor(newData.get / state.get()) >= 2) {
          var name = "Goldman Sachs"
          if (key.equals("citigroup")) {
            name = "Citigroup"
          }
          println(s"Number of arrivals to $name has doubled from ${state.get()} to ${newData.get} at ${time.milliseconds}!")
        }
      }

      state.update(newData.getOrElse(0))

      Some((key, (newData.getOrElse(0), time.milliseconds, s)))
    }

    val rdds = buildMockStream(ssc.sparkContext, args.input())
    val inputData: mutable.Queue[RDD[String]] = mutable.Queue()
    val stream = ssc.queueStream(inputData)

    val wc = stream.map(_.split(","))
      .flatMap(line =>
        if (line(0).equals("yellow")) {
          if (line(10).toDouble < -74.013777 && line(10).toDouble > -74.0144185 &&
            line(11).toDouble > 40.7138745 && line(11).toDouble < 40.7152275) { //check for goldman
            List(("goldman", 1))
          } else if (line(10).toDouble < -74.009867 && line(10).toDouble > -74.012083 &&
            line(11).toDouble > 40.720053 && line(11).toDouble < 40.7217236) {
            List(("citigroup", 1))
          } else {
            List()
          }
        } else {
          if (line(8).toDouble < -74.013777 && line(8).toDouble > -74.0144185 &&
            line(9).toDouble > 40.7138745 && line(9).toDouble < 40.7152275) { //check for goldman
            List(("goldman", 1))
          } else if (line(8).toDouble < -74.009867 && line(8).toDouble > -74.012083 &&
            line(9).toDouble > 40.720053 && line(9).toDouble < 40.7217236) {
            List(("citigroup", 1))
          } else {
            List()
          }
        })
      .reduceByKeyAndWindow((x: Int, y: Int) => x + y, (x: Int, y: Int) => x - y, Minutes(10), Minutes(10))
      .mapWithState(StateSpec.function(stateUpdateFunction _))
      .print()
    //.persist()

    //    wc.saveAsTextFiles(args.output())
    //
    //    wc.foreachRDD(rdd => {
    //      numCompletedRDDs.add(1L)
    //    })
    ssc.checkpoint(args.checkpoint())
    ssc.start()

    for (rdd <- rdds) {
      inputData += rdd
      ManualClockWrapper.advanceManualClock(ssc, batchDuration.milliseconds, 50L)
    }

    batchListener.waitUntilCompleted(() =>
      ssc.stop()
    )
  }

  class StreamingContextBatchCompletionListener(val ssc: StreamingContext, val limit: Int) extends StreamingListener {
    def waitUntilCompleted(cleanUpFunc: () => Unit): Unit = {
      while (!sparkExSeen) {}
      cleanUpFunc()
    }

    val numBatchesExecuted = new AtomicInteger(0)
    @volatile var sparkExSeen = false

    override def onBatchCompleted(batchCompleted: StreamingListenerBatchCompleted) {
      val curNumBatches = numBatchesExecuted.incrementAndGet()
      log.info(s"${curNumBatches} batches have been executed")
      if (curNumBatches == limit) {
        sparkExSeen = true
      }
    }
  }

  def buildMockStream(sc: SparkContext, directoryName: String): Array[RDD[String]] = {
    val d = new File(directoryName)
    if (d.exists() && d.isDirectory) {
      d.listFiles
        .filter(file => file.isFile && file.getName.startsWith("part-"))
        .map(file => d.getAbsolutePath + "/" + file.getName).sorted
        .map(path => sc.textFile(path))
    } else {
      throw new IllegalArgumentException(s"$directoryName is not a valid directory containing part files!")
    }
  }
}

