package com.deep.kafka;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducerWithCallback {


    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(KafkaProducerWithCallback.class);
        String bootStrapServer="localhost:9092";

        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootStrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(properties);

        //Create a producer record
        ProducerRecord<String,String> record = new ProducerRecord<>("first_topic","hellow from java");

        //send message
        kafkaProducer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if(e==null){
                    logger.info(" topic " + recordMetadata.topic());
                    System.out.println(" topic " + recordMetadata.topic());
                    System.out.println(" partition "+ recordMetadata.partition());
                    logger.info(" partition "+ recordMetadata.partition()) ;
                }
                else{
                    logger.error("error occured");
                }
            }
        });

        kafkaProducer.flush();
        kafkaProducer.close();
    }
}
