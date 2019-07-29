package com.practice.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * @author  Velmurugan Moorthy
 * 29.07.2019 - 04.25 PM
 */
public class KafkaProducerDemoWithKeys {

    public static void main(String[] args) {

        // Create producer properties
        Properties simpleProducerProperties = new Properties();
        populateProducerProperties(simpleProducerProperties);

        // create Producer
        KafkaProducer<String,String> simpleKafkaProducer = new KafkaProducer<String, String>(simpleProducerProperties);

        // Create producer record
        for(int loopIndex = 0 ; loopIndex < 10 ; loopIndex++){
            String key = "id_" + loopIndex;
            String value = "Kafka producer with Keys : " + loopIndex;
            System.out.println("Key : "+key);
            ProducerRecord<String,String> kafkaProducerRecord = new ProducerRecord<String, String>(
                    KafkaDemoProducerConstants.KAFKA_TOPIC_NAME, key, value);
            simpleKafkaProducer.send(kafkaProducerRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    if( null == e){
                        System.out.println("\n Meta deta : ");
                        System.out.println("Topic : "+recordMetadata.topic());
                        System.out.println("Partition : " + recordMetadata.partition());
                        System.out.println("Offset : "+recordMetadata.offset());
                        System.out.println("Timestamp : "+recordMetadata.timestamp());
                    }
                    else {
                        System.out.println("Error occured : "+ e);
                    }
                }
            });
        }

        // Flush producer
        simpleKafkaProducer.flush();
        // Close producer.
        simpleKafkaProducer.close();

    }

    private static void populateProducerProperties(Properties simpleProducerProperties) {
        simpleProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
                KafkaDemoProducerConstants.BOOTSTRAP_SERVERS);
        simpleProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        simpleProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
    }
}
