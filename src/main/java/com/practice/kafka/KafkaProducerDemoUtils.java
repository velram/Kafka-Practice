package com.practice.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author  Velmurugan Moorthy
 * 31.07.2019
 */
public class KafkaProducerDemoUtils {

    public static Properties fetchKafkaProducerProperties(){
        Properties kafkaProducerProperties = new Properties();
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaDemoProducerConstants.BOOTSTRAP_SERVERS);
        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        return kafkaProducerProperties;
    }

    public static KafkaProducer<String, String> createKafkaProducer() {

        Properties kafkaProperties = fetchKafkaProducerProperties();
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

        return kafkaProducer;
    }
}
