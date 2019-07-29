package com.practice.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerDemo {

    private static final  String BOOTSTRAP_SERVERS = "localhost:9092";
    private static final String MESSAGE_TO_SEND = "Message from Java code - Kafka (API) - 2";

    public static void main(String[] args) {

        System.out.println("Kafka producer demo starts : ");


        //Create Producer properties
        Properties producerProperties = new Properties();

        populateProducerProperties(producerProperties);

        //Create Producer

        KafkaProducer<String,String> demoKafkaProducer = new KafkaProducer<String, String>(producerProperties);

        // Create Kafka record

        ProducerRecord<String,String> producerRecord = new ProducerRecord<String, String>("first-topic",MESSAGE_TO_SEND);

        // Send the data
        demoKafkaProducer.send(producerRecord);

        // Flush the producer
        demoKafkaProducer.flush();

        // Close the producer
        demoKafkaProducer.close();
    }

    /**
     * Invoked during Kafka producer creation
     * @param producerProperties
     */
    private static void populateProducerProperties(Properties producerProperties) {
        producerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        producerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }
}
