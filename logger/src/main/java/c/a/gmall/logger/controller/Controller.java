package c.a.gmall.logger.controller;

import c.a.gmall.constant.Constants;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * @author ???
 * 2019-04-28 15:02
 */
@RestController
public class Controller {

    @Autowired
    private KafkaTemplate kafkaTemplate;

    private static final  org.slf4j.Logger logger = LoggerFactory.getLogger(Controller.class) ;

    @ResponseBody
    @RequestMapping("log")
    public String getLog(@RequestParam("log") String jsonLog){
        JSONObject jsonObject = JSON.parseObject(jsonLog);
        jsonObject.put("ts", System.currentTimeMillis());
        if("startup".equals(jsonObject.getString("type"))){
            kafkaTemplate.send(Constants.KAFKA_TOPIC_STARTUP, jsonObject.toJSONString());
            logger.info(jsonObject.toJSONString());
        }
        return "success!!!";
    }


}
