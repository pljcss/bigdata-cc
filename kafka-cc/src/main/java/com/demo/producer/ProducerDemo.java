package com.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author cs
 * @date 2020/10/31 2:58 下午
 */
public class ProducerDemo {
    public static void main(String[] args) throws IOException {

        InputStream resourceAsStream = ProducerDemo.class.getClassLoader()
                .getResourceAsStream("kafka-config.properties");
        Properties properties = new Properties();
        properties.load(resourceAsStream);


        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        producer.send(new ProducerRecord<>("test111-topic", "one record 111111"));

        producer.close();

    }
}
