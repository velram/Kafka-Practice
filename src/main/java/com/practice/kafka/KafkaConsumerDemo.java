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

public class KafkaConsumerDemo {

    private static final String KAFKA_CONSUMER_GROUP_ID = "my-second-application-group";

    public static void main(String[] args) {

        Logger loggerObject = LoggerFactory.getLogger(KafkaConsumerDemo.class.getName());

        // Create properties
        Properties kafkaConsumerProperties = KafkaConsumerDemoUtils.fetchKafkaConsumerProperties(
                                             KafkaDemoProducerConstants.KAFKA_CONSUMER_GROUP_ID,
                                             KafkaDemoProducerConstants.KAFKA_CONSUMER_OFFSET_AUTO_REST_MODE);

        //Create consumer
        KafkaConsumer<String,String> simpleKafkaConsumer = new KafkaConsumer<String, String>
                (kafkaConsumerProperties);

        // subscribe consumer to topics.
        simpleKafkaConsumer.subscribe(
                Arrays.asList(KafkaDemoProducerConstants.KAFKA_TOPIC_NAME)
        );

        // Create consumer records
        while(true){
            ConsumerRecords<String,String> consumerRecords = simpleKafkaConsumer.poll(
                    Duration.ofMillis(100));
            for (ConsumerRecord<String,String> consumerRecord:
                 consumerRecords) {
                loggerObject.info("Key : " + consumerRecord.key() + "\n" +
                        "Value : " + consumerRecord.value() + "\n");
                loggerObject.info("Topic : " + consumerRecord.topic() + "\n" +
                                  "Partition : " + consumerRecord.partition() + "\n" +
                                  "Timestamp : " + consumerRecord.timestamp() + "\n" +
                                  "Offset : " + consumerRecord.offset() + "\n"
                );
            }
        }

    }

    public static Properties fetchKafkaConsumerGroupProperties() {
        Properties kafkaConsumerGroupProperties = new Properties();
        kafkaConsumerGroupProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaDemoProducerConstants.BOOTSTRAP_SERVERS);
        kafkaConsumerGroupProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        kafkaConsumerGroupProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());
        kafkaConsumerGroupProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,
                KAFKA_CONSUMER_GROUP_ID);
        kafkaConsumerGroupProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,
                KafkaDemoProducerConstants.KAFKA_CONSUMER_OFFSET_AUTO_REST_MODE);

        return kafkaConsumerGroupProperties;
    }


}
