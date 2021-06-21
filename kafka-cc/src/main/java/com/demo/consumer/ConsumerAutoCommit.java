package com.demo.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

/**
 * @author cs
 * @date 2020/11/1 1:44 下午
 * 简单消费者
 * 自动提交 offset
 */
public class ConsumerAutoCommit {
    public static void main(String[] args) {
        // 创建kafka消费者配置信息
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.5.3:9092");
        // 消费者
        props.setProperty("group.id", "test");
        // 开启自动提交
        props.setProperty("enable.auto.commit", "true");
        // 自动提交延迟
        props.setProperty("auto.commit.interval.ms", "1000");
        // 序列化、反序列化
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 创建消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅topic
        consumer.subscribe(Arrays.asList("hello-topic"));

        // 消费数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic=%s, partition=%d, offset = %d, key = %s, value = %s%n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }
        }

    }
}
