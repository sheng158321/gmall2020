package com.atguigu.handler

import java.text.SimpleDateFormat
import java.util
import java.util.Date

import com.atguigu.bean.StartUpLog
import com.atguigu.utils.RedisUtil
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object DauHandler1 {

  //同批次去重，redis去重之后的数据集
  def filterByMid(filterdByRedis: DStream[StartUpLog]) = {

    val midDateToLogDStream: DStream[((String, String), StartUpLog)] = filterdByRedis.map(
      log => ((log.mid, log.logDate), log)
    )

    val midDateToLogInterDStream: DStream[((String, String), Iterable[StartUpLog])] = midDateToLogDStream.groupByKey()

//    val midDateToLogListDStream: DStream[((String, String), List[StartUpLog])] = midDateToLogInterDStream.mapValues(
//      iter => {
//        iter.toList.sortWith(_.ts < _.ts).take(1)
//      }
//    )
//
//    midDateToLogListDStream.flatMap(_._2)

    midDateToLogInterDStream.flatMap {
      case ((mid, date), iter) => {
        iter.toList.sortWith(_.ts < _.ts).take(1)
      }
    }
  }


  private val sdf = new SimpleDateFormat("yyyy-MM-dd")

  /**
   * 根据redis去重
   * 原始数据
   **/

  def filterByRedis(startLogDStream: DStream[StartUpLog], sc: SparkContext): DStream[StartUpLog] = {

    //一、filter单条数据过滤
    //    val value1: DStream[StartUpLog] = startLogDStream.filter(
    //      log => {
    //
    //        val jedisClient: Jedis = RedisUtil.getJedisClient
    //        //判断数据是否存在
    //        val redisKey = s"${log.logDate}"
    //        val boolean: lang.Boolean = jedisClient.sismember(redisKey, log.mid)
    //
    //        jedisClient.close()
    //
    //        !boolean
    //      }
    //    )
    //
    //    value1


    //分区内获取连接
    //    startLogDStream.transform(rdd => {
    //        rdd.mapPartitions(
    //          iter => {
    //            val jedisClient: Jedis = RedisUtil.getJedisClient
    //
    //            val logs: Iterator[StartUpLog] = iter.filter(
    //              log => {
    //                val redisKey = s"${log.logDate}"
    //                !jedisClient.sismember(redisKey, log.mid)
    //              }
    //            )
    //            jedisClient.close()
    //            logs
    //          }
    //        )
    //      }
    //    )


    //一个批次获取一次连接，Driver端获取数据广播至Executor
    startLogDStream.transform(rdd => {
      val jedisClient: Jedis = RedisUtil.getJedisClient

      val mids: util.Set[String] = jedisClient.smembers(s"DAU:${sdf.format(new Date(System.currentTimeMillis()))}")

      jedisClient.close()

      val midsBC: Broadcast[util.Set[String]] = sc.broadcast(mids)

      rdd.filter(
        log => {
          !midsBC.value.contains(log.mid)
        }
      )
    }
    )


  }

  /**
   * 去重之后的数据Mid保存到Redis
   * 两次去重之后的数据集
   **/
  def savemidToRedis(startLogDStream: DStream[StartUpLog]) = {
    startLogDStream.foreachRDD(
      rdd => {
        //分区操作，减少来连接的获取与释放
        rdd.foreachPartition(
          iter => {
            //获取连接
            val jedisClient: Jedis = RedisUtil.getJedisClient
            //遍历写入
            iter.foreach(
              log => {
                val redisKey = s"DAU:${log.logDate}"
                jedisClient.sadd(redisKey, log.mid)
              }
            )

            //归还
            jedisClient.close()
          }
        )
      }
    )
  }

}
