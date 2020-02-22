package com.github.praveen.kafka.tutorial3;

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

public class ElasticSearchConsumer {

    public static KafkaConsumer<String,String> createConsumer(String topic){
        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "kafka-demo-elasticsearch";

        // create consumer configs
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest"); // earliest/latest/none
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false"); // This is to disable the auto commit of the offsets
        properties.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"10");

        // create consumer
        KafkaConsumer<String,String> consumer = new KafkaConsumer<String, String>(properties);
        consumer.subscribe(Arrays.asList(topic));
        return consumer;
    }

    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());
        KafkaConsumer<String,String> consumer = createConsumer("tweeter_tweets");

        // poll for new data
        while(true){ // bad implementation

            ConsumerRecords<String,String> records = consumer.poll(Duration.ofMillis(100)); // new in kafka 2.0.0 or later

            logger.info("Received: "+records.count()+ " records");


            for(ConsumerRecord<String,String> record : records){
               // where we insert the data into ElasticSearch
                String jsonTweet = record.value();

                try {
                    Thread.sleep(1000); // Sleep for 1000 milli seconds
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }

            logger.info("Committing the offset");
            consumer.commitAsync();  // manually committing the offsets.
            logger.info("Offsets have been committed");
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

    }
}
