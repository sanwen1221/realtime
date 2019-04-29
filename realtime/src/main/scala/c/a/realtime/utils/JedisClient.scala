package c.a.realtime.utils

import java.util.Properties

import redis.clients.jedis.Jedis

/**
  * @author ???
  *         2019-04-29 18:38
  */
object JedisClient {

    def getJedis()={
        val properties: Properties = PropertiesUtils.load("config.properties")
        val host: String = properties.getProperty("redis.host")
        val port: String = properties.getProperty("redis.port")
        val jedis = new Jedis(host,port.toInt)
        jedis
    }

}
