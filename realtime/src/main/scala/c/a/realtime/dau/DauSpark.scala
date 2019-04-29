package c.a.realtime.dau

import java.text.SimpleDateFormat
import java.util
import java.util.{Date, Properties}

import c.a.gmall.constant.Constants
import c.a.gmall.utils.MyEsUtil
import c.a.realtime.caseclass.User
import c.a.realtime.utils.{JedisClient, MyKafkaUtils, PropertiesUtils}
import com.alibaba.fastjson.JSON
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis



/**
  * @author ???
  *         2019-04-29 16:50
  */
object DauSpark {
    def main(args: Array[String]): Unit = {

        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DAU")

        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtils.getKafkaStream(Constants.KAFKA_TOPIC_STARTUP,ssc)


        val value: DStream[User] = inputDStream.map(consumerRecord => {
            val str: String = consumerRecord.value()
            val user: User = JSON.parseObject(str, classOf[User])
            val dateTime: String = new SimpleDateFormat("yyyy-MM-dd hh:mm").format(new Date())
            user.logDate=dateTime.split(" ")(0)
            user.logHour=dateTime.split(" ")(1).split(":")(0)
            user.logHourMinute=dateTime.split(" ")(1)
            user
        })

        //首先去除redis中已经有的用户，然后将redis中没有并且重复的活跃用户
        val transDS: DStream[User] = value.transform(rdd => {

            println("访问人数：" + rdd.count())
            val jedis: Jedis = JedisClient.getJedis()
            //获取当天已经活跃的用户们
            val users: util.Set[String] = jedis.smembers("dau:" + new SimpleDateFormat("yyyy-MM-dd").format(new Date()))
            //将其广播,用于去重
            val broadcast: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(users)

            val filterrdd: RDD[User] = rdd.filter(user => {
                val users: util.Set[String] = broadcast.value
                !users.contains(user.mid)
            })

            jedis.close()
            val newRdd: RDD[User] = filterrdd.map(user=>{(user.mid,user)}).groupByKey()flatMap(_._2.take(1))
            println("新增访问数：" + newRdd.count())
            newRdd
        })


        //把数据在每一个executor中按照周期放入放入redis中
        transDS.foreachRDD(rdd=>{
            //将rdd的数据存入ES中
            val list: List[User] = rdd.collect().toList
            MyEsUtil.insertEsBulk("gmall1111_dau",list)
            rdd.foreachPartition(iterator=>{
                val jedis: Jedis = JedisClient.getJedis()
                iterator.foreach(user=>{
                    jedis.sadd("dau:"+new SimpleDateFormat("yyyy-MM-dd").format(new Date()),user.mid)
                })
                jedis.close()

            })
        })

        ssc.start()
        ssc.awaitTermination()
    }

}
