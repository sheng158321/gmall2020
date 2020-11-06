package com.atguigu.app

import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.InputDStream
import org.apache.spark.streaming.{Seconds, StreamingContext}

object RealtimeStartupApp {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("RealtimeStartupApp")

    val ssc = new StreamingContext(conf, Seconds(5))

    val startupStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, GmallConstants.KAFKA_TOPIC_STARTUP)

    startupStream.foreachRDD(
      rdd=>{
        rdd.foreach(
          record=>{
            println(record.value())
          }
        )
      }
    )

    //    val startupLogDstream: DStream[StartUpLog] = startupStream.map(_.value()).map { log =>
    //      // println(s"log = ${log}")
    //      val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
    //      startUpLog
    //    }


        ssc.start()
        ssc.awaitTermination()
  }
}
