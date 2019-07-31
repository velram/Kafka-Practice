package com.practice.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

/**
 * @author Velmurugan Moorthy
 * 30.07.2019
 * This class will be used during the creation of Kafka Consumer
 */
public class KafkaConsumerDemoUtils {

    public static Properties fetchKafkaConsumerProperties(String kafkaConsumerGroupId, String autoOffsetResetMode) {
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaDemoProducerConstants.BOOTSTRAP_SERVERS);
        kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                kafkaConsumerGroupId);
        kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                autoOffsetResetMode);

        return kafkaConsumerProperties;
    }

}
