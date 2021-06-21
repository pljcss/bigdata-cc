package com.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.Config;

import java.util.Properties;

/**
 * @Author: cs
 * @Date: 2021/4/16 9:55 下午
 * @Desc:
 */
public class Test {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.5.3:9092");
        props.setProperty(ProducerConfig.ACKS_CONFIG, "-1");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int i = 0; i < 10000; i++) {
            producer.send(new ProducerRecord<String, String>("hello-topic",
                    i + "-value",
                    i + "-value"));

            Thread.sleep(100);
        }

        producer.close();


    }
}
