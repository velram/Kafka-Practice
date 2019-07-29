package com.practice.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * @author velmurugan.moorthy
 * date : 29.07.2019 - 02.23 PM
 */
public class KafkaProducerWithCallbackDemo {

    public static void main(String[] args) {

       final Logger loggerObject = LoggerFactory.getLogger(KafkaProducerWithCallbackDemo.class);

        // Create Producer properties
        Properties kafkaProducerProperties = new Properties();

        populateProducerConfigProperties(kafkaProducerProperties);

        // Create Kafka producer
        KafkaProducer<String,String> simpleKafkaProducer = new KafkaProducer<String, String>(kafkaProducerProperties);

        // Create producer record
        for (int loopIndex = 0 ; loopIndex < 10 ; loopIndex++){
            ProducerRecord<String,String> simpleKafkaRecord = new ProducerRecord<String, String>
                    (KafkaDemoProducerConstants.KAFKA_TOPIC_NAME,"Message number : "+loopIndex);
            simpleKafkaProducer.send(simpleKafkaRecord, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                  if(null == e){

                      loggerObject.info( "Record's meta data is : " + "\n" +
                              "Topic : " + recordMetadata.topic() + "\n" +
                      "Partition : "+recordMetadata.partition() + "\n" +
                      "offset : "+recordMetadata.offset() + "\n" +
                      "Timestamp : "+recordMetadata.timestamp() + "\n");
                  }
                  else {

                  }
                }
            });
        }

        // Flush the producer
        simpleKafkaProducer.flush();
        // Close the producer
        simpleKafkaProducer.close();

    }

    private static void populateProducerConfigProperties(Properties kafkaProducerProperties) {
        kafkaProducerProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaDemoProducerConstants.BOOTSTRAP_SERVERS);
        kafkaProducerProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProducerProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
    }
}
