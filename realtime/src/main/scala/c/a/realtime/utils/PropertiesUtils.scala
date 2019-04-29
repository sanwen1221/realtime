package c.a.realtime.utils

import java.io.InputStreamReader
import java.util.Properties

/**
  * @author ???
  *         2019-04-29 12:34
  */
object PropertiesUtils {



    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtils.load("config.properties")

        println(properties.getProperty("kafka.broker.list"))
    }

    def load(propertieName:String): Properties ={
        val prop=new Properties();
        prop.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(propertieName) , "UTF-8"))
        prop
    }


}