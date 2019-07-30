package com.practice.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class KafkaConsumerDemoGroups {

    private static final String KAFKA_CONSUMER_GROUP_ID = "my-third-consumer-group";

    public static void main(String[] args) {

        Logger loggerObject = LoggerFactory.getLogger(KafkaConsumerDemoGroups.class.getName());
        // Create Kafka properties
        Properties kafkaConsumerProps = fetchKafkaConsumerGroupProperties();

        // Create Consumer
        KafkaConsumer<String,String> simpleKafkaConsumer = new KafkaConsumer<String, String>(kafkaConsumerProps);

        // Create Consumer record

        // Subscribe to topic
        simpleKafkaConsumer.subscribe(Arrays.asList(KafkaDemoProducerConstants.KAFKA_TOPIC_NAME));

        // start consuming data
        while(true){
            ConsumerRecords<String,String> consumerRecords = simpleKafkaConsumer.poll(
                    Duration.ofMillis(100));
            for (ConsumerRecord<String,String> consumerRecord:
                    consumerRecords) {
                loggerObject.info("Key : " + consumerRecord.key() + "\n" +
                        "Value : " + consumerRecord.value() + "\n");
                loggerObject.info("Topic : " + consumerRecord.topic() + "\n" +
                        "Partition : " + consumerRecord.partition() + "\n" +
                        "Offset : " + consumerRecord.offset() + "\n" +
                        "Timestamp : " + consumerRecord.timestamp() + "\n"
                );
            }
        }
        // flush & close
    }

    public static Properties fetchKafkaConsumerGroupProperties() {
        Properties kafkaConsumerProperties = new Properties();
        kafkaConsumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaDemoProducerConstants.BOOTSTRAP_SERVERS);
        kafkaConsumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        kafkaConsumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                KAFKA_CONSUMER_GROUP_ID);
        kafkaConsumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        kafkaConsumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                KafkaDemoProducerConstants.KAFKA_CONSUMER_OFFSET_AUTO_REST_MODE);

        return kafkaConsumerProperties;
    }
}
