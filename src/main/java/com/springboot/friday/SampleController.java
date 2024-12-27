package com.springboot.friday;

import com.springboot.kafka.IKafkaConsumer;
import com.springboot.kafka.IKafkaProducer;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.env.Environment;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Properties;


@RestController
public class SampleController {
    private final com.springboot.kafka.IKafkaProducer IKafkaProducer = new IKafkaProducer();
    @Autowired
    public Environment environment;


    @GetMapping("/welcome")
    public String process() {
        /*IKafkaProducer.produceMsg("sampleMessages", "localhost:29092", "key1", "Hello, Kafka!");
        return getConsumedMsg("sampleMessages", environment.getProperty("spring.kafka.bootstrap-servers"));*/
        return environment.getProperty("spring.kafka.bootstrap-servers");
    }

    @GetMapping("/name")
    public String getName(){
        return "Nagendra";
    }

    private List<String> getConsumedMsg(String topic, String boostrapServer) {
        Properties prop = IKafkaConsumer.loadproperties(boostrapServer);
        IKafkaConsumer kafkaConsumer = new IKafkaConsumer(prop);
        return kafkaConsumer.getMsg(topic, 100);
    }

}