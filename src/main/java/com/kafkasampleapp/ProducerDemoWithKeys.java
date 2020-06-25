package com.kafkasampleapp;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoWithKeys {

    private static final Logger logger = LoggerFactory.getLogger(ProducerDemoWithKeys.class);
    public static void main(String[] args) throws ExecutionException, InterruptedException {

        // create producer properties
        String serverAddress = "localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, serverAddress);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        for (int i=0; i< 10; i++) {

            String key = "id_" + i;
            // create a Producer record
            ProducerRecord<String, String> record =
                    new ProducerRecord<>("first_topic", key ,"hello world " + i);

            logger.info("Key {}", key);
            // send data - asynchronous
            //producer.send(record);

            // send data and receive callback
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if (e != null) {
                        logger.error("Error while sending message to Kafka topic");
                    } else {
                        logger.info("Topic : " + recordMetadata.topic() +
                                " Partition : " + recordMetadata.partition() +
                                " Offset : "  + recordMetadata.offset());
                    }
                }
            }).get(); // blocking call.. don't do in production
        }

        // flush
        producer.flush();
        // close
        producer.close();
    }
}
