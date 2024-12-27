package com.springboot.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class IKafkaProducer {
    public IKafkaProducer() {
    }

    public void produceMsg(String topic, String boostrapServer, String key, String value) {
        // Kafka broker address

        // Configure producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, boostrapServer);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

        // Create Kafka Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

        try {
            // Create a ProducerRecord (message) with topic, key, and value
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            // Send message synchronously
            RecordMetadata metadata = producer.send(record).get(); // .get() makes it synchronous
            System.out.printf("Message sent to topic %s, partition %d, offset %d%n",
                    metadata.topic(), metadata.partition(), metadata.offset());
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
        } finally {
            // Close producer
            producer.close();
        }
    }
}