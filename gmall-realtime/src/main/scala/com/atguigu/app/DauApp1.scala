package com.atguigu.app

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.phoenix.spark._
import com.alibaba.fastjson.JSON
import com.atguigu.bean.StartUpLog
import com.atguigu.constants.GmallConstants
import com.atguigu.handler.DauHandler1
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object DauApp1 {
  def main(args: Array[String]): Unit = {
    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")

    val ssc = new StreamingContext(conf, Seconds(5))

    //消费kafka数据
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, GmallConstants.KAFKA_TOPIC_STARTUP)

    //将每一行数据转换为样例类对象，并添加时间字段
    //    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    //    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(
    //      record => {
    //        //获取value
    //        val value: String = record.value()
    //        //取出时间戳字段
    //        val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
    //        val ts: Long = startUpLog.ts
    //        //转换为字符串
    //        val startUpHour: String = sdf.format(new Date(ts))
    //        val dateArr: Array[String] = startUpHour.split(" ")
    //
    //        startUpLog.logDate = dateArr(0)
    //        startUpLog.logHour = dateArr(1)
    //        startUpLog
    //
    //      }
    //    )

    //将每一行数据转换为样例类对象，并添加时间字段
    val sdf = new SimpleDateFormat("yyyy-MM-dd HH")
    val startLogDStream: DStream[StartUpLog] = kafkaDStream.map(record => {
      val value: String = record.value()

      val startUpLog: StartUpLog = JSON.parseObject(value, classOf[StartUpLog])
      val ts: Long = startUpLog.ts
      val dateHourStr: String = sdf.format(new Date(ts))

      val dateHourArr: Array[String] = dateHourStr.split(" ")

      startUpLog.logDate = dateHourArr(0)
      startUpLog.logHour = dateHourArr(1)

      startUpLog
    })

    //Redis跨批次去重
    val filterdByRedis: DStream[StartUpLog] = DauHandler1.filterByRedis(startLogDStream,ssc.sparkContext)

//        startLogDStream.cache()
//        startLogDStream.count().print()
//        filterdByRedis.cache()
//        filterdByRedis.count().print()

    //同批次去重
    val filterdByMid: DStream[StartUpLog] = DauHandler1.filterByMid(filterdByRedis)

//    filterdByMid.cache()
//    filterdByMid.count().print()


    //去重后的Mid保存到Redis
    DauHandler1.savemidToRedis(filterdByMid)

    //去重后的数据明细写入Pheonix
    filterdByMid.foreachRDD(
      rdd=>{
        rdd.saveToPhoenix("GMALL2020_DAU",
          Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS"),
          HBaseConfiguration.create(),
          Some("hadoop102,hadoop103,hadoop104:2181"))
      }
    )


    ssc.start()
    ssc.awaitTermination()
  }
}
