package com.springboot.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

public class IKafkaConsumer {

    private final KafkaConsumer<String, String> consumer;

    public IKafkaConsumer(Properties props) {
        this.consumer = new KafkaConsumer<>(props);
    }


    public List<String> getMsg(String topic, int seconds) {
        List<String> messages = new ArrayList<>();
        consumer.subscribe(Collections.singletonList(topic));

        long startTime = System.currentTimeMillis();
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(seconds));
        long endTime = System.currentTimeMillis();
        System.out.println("Poll duration: " + (endTime - startTime) + "ms");

        for (ConsumerRecord<String, String> record : records) {
            messages.add(record.value());
        }
        return messages;
    }

    public synchronized List<String> fetchMessages(String topic, int seconds) {
        consumer.assign(Collections.singletonList(new TopicPartition("sampleMessages", 1)));
        long startTime = System.currentTimeMillis();
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(seconds));
        long endTime = System.currentTimeMillis();
        System.out.println("Poll duration: " + (endTime - startTime) + "ms");
        return StreamSupport.stream(records.spliterator(), false)
                .map(ConsumerRecord::value)
                .collect(Collectors.toList());
    }


    public static Properties loadproperties(String boostrapServer) {
        Properties props = new Properties();
        props.put("bootstrap.servers", boostrapServer);
        props.put("group.id", "group_id");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("auto.offset.reset", "earliest");
        props.put("fetch.min.bytes","1");
        props.put("fetch.max.wait.ms","500");
        return props;
    }
}



















