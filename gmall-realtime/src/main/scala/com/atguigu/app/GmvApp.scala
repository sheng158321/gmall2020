package com.atguigu.app

import org.apache.phoenix.spark._
import com.alibaba.fastjson.JSON
import com.atguigu.bean.OrderInfo
import com.atguigu.constants.GmallConstants
import com.atguigu.utils.MyKafkaUtil
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object GmvApp {
  def main(args: Array[String]): Unit = {

    //创建sparkconf
    val conf: SparkConf = new SparkConf().setAppName("GmvApp").setMaster("local[*]")
    //创建stremingcontext
    val ssc = new StreamingContext(conf, Seconds(5))
    //消费kafka主题
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(ssc, GmallConstants.KAFKA_ORDER_INFO)
    //每行转换为样例类，补充时间，脱敏
    val orderInfoDStream: DStream[OrderInfo] = kafkaDStream.map(
      record => {
        //将value转换为样例类
        val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])
        //取出时间 yyyy-MM-dd HH:mm:ss
        val create_time: String = orderInfo.create_time
        //给时间重新赋值
        val arr: Array[String] = create_time.split(" ")
        orderInfo.create_date = arr(0)
        orderInfo.create_hour = arr(1).split(":")(0)
        //数据脱敏
        val consignee_tel: String = orderInfo.consignee_tel
        val tuple: (String, String) = consignee_tel.splitAt(4)
        orderInfo.consignee_tel = tuple._1 + "********"
        //返回结果
        orderInfo
      }
    )


    /**
     * Seq("ID", "PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL",
     * "ORDER_STATUS", "PAYMENT_WAY", "USER_ID", "IMG_URL", "TOTAL_AMOUNT",
     * "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME", "OPERATE_TIME",
     * "TRACKING_NO", "PARENT_ORDER_ID", "OUT_TRADE_NO", "TRADE_BODY",
     * "CREATE_DATE", "CREATE_HOUR")
     **/
    //写入phoenix
    orderInfoDStream.foreachRDD(rdd => {
      println("aaaaaaaaaaaa")
      rdd.saveToPhoenix("GMALL2020_ORDER_INFO",
        classOf[OrderInfo].getDeclaredFields.map(_.getName.toUpperCase),
        HBaseConfiguration.create(),
        Some("hadoop102,hadoop103,hadoop104:2181"))
    })

    //开启任务
    ssc.start()
    ssc.awaitTermination()
  }
}
