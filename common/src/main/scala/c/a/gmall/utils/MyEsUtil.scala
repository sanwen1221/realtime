package c.a.gmall.utils

import java.util.Objects

import com.alibaba.fastjson.JSON
import io.searchbox.client.config.HttpClientConfig
import io.searchbox.client.{JestClient, JestClientFactory}
import io.searchbox.core.{Bulk, BulkResult, Index}



/**
  * @author ???
  *         2019-04-29 21:21
  */
object MyEsUtil {
    private val ES_HOST = "http://hadoop102"
    private val ES_HTTP_PORT = 9200
    private var factory:JestClientFactory = null

    /**
      * 获取客户端
      *
      * @return jestclient
      */
    def getClient: JestClient = {
        if (factory == null) build()
        factory.getObject
    }

    /**
      * 关闭客户端
      */
    def close(client: JestClient): Unit = {
        if (!Objects.isNull(client)) try
            client.shutdownClient()
        catch {
            case e: Exception =>
                e.printStackTrace()
        }
    }

    /**
      * 建立连接
      */
    private def build(): Unit = {
        factory = new JestClientFactory
        factory.setHttpClientConfig(new HttpClientConfig.Builder(ES_HOST + ":" + ES_HTTP_PORT).multiThreaded(true)
          .maxTotalConnection(20) //连接总数
          .connTimeout(10000).readTimeout(10000).build)

    }

    def main(args: Array[String]): Unit = {

        val map1 = Map("name"->"wangwu","movietime"->"3.3")
        val map2 = Map("name"->"zhaoliu","movietime"->"4.4")
        val map3 = Map("name"->"qianqi","movietime"->"5.5")

        val list =List(map1,map2,map3)
        insertEsBulk("movie_1111",list)


    }

    def insertEsBulk(indexName:String,list: List[Any])={
        val client: JestClient = getClient

        val builder = new Bulk.Builder()
        //设置默认的index和type
        builder.defaultIndex(indexName).defaultType("_doc")
        for (elem <- list) {

            val index: Index = new Index.Builder(elem).build()
            builder.addAction(index)
        }
        val bulk: Bulk = builder.build()

        client.execute(bulk)
        close(client)
    }


    /*def executeIndexBulk(indexName:String ,list:List[Any], idColumn:String): Unit ={
        val bulkBuilder: Bulk.Builder = new Bulk.Builder().defaultIndex(indexName).defaultType("_doc")
        for ( doc <- list ) {

            val indexBuilder = new Index.Builder(doc)
            if(idColumn!=null){
                val id: String = BeanUtils.getProperty(doc,idColumn)
                indexBuilder.id(id)
            }
            val index: Index = indexBuilder.build()
            bulkBuilder.addAction(index)
        }
        val jestclient: JestClient =  getClient

        val result: BulkResult = jestclient.execute(bulkBuilder.build())
        if(result.isSucceeded){
            println("保存成功:"+result.getItems.size())
        }

    }*/
}


