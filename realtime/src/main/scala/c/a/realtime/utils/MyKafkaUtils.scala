package c.a.realtime.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}

/**
  * @author ???
  *         2019-04-29 12:36
  */
object MyKafkaUtils {
    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("kafkaDStream")

        val ssc = new StreamingContext(sparkConf,Seconds(5))

        val inputDStream: InputDStream[ConsumerRecord[String, String]] = getKafkaStream("startup",ssc)

        val value: DStream[String] = inputDStream.map(consumerRecord => {
            consumerRecord.value()
        })
        value.print()
        ssc.start()
        ssc.awaitTermination()
    }

    private val properties: Properties = PropertiesUtils.load("config.properties")
    val broker_list = properties.getProperty("kafka.broker.list")

    val kafkaParam=Map(
        "bootstrap.servers"->broker_list,
        "key.deserializer"->classOf[StringDeserializer],
        "value.deserializer"->classOf[StringDeserializer],
        "group.id" -> "gmall_consumer_group",
        "auto.offset.reset" -> "latest",
        "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    def getKafkaStream(topic:String,ssc :StreamingContext)={
        val dStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(ssc,LocationStrategies.PreferConsistent,ConsumerStrategies.Subscribe[String,String](Array(topic),kafkaParam))
        dStream
    }
}
