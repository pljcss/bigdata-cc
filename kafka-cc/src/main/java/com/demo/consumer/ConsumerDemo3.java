package com.demo.consumer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

/**
 * 关闭offset自动提交
 * @author cs
 * @date 2020/11/1 1:44 下午
 */
public class ConsumerDemo3 {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        // 消费者组
        props.setProperty("group.id", "test333");
        // 关闭自动提交
        props.setProperty("enable.auto.commit", "false");

        props.setProperty("auto.commit.interval.ms", "1000");
        props.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        // 重置消费者的offset，注意生效条件，比如换组才生效
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 创建消费者实例
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // 订阅topic
        consumer.subscribe(Arrays.asList("test2-topic", "test3-topic"));

        // 消费数据
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("topic=%s, partition=%d, offset = %d, key = %s, value = %s%n",
                        record.topic(), record.partition(), record.offset(), record.key(), record.value());
            }

            // 同步提交，当前线程会阻塞直到offset提交成功
//            consumer.commitSync();

            // 异步提交
            consumer.commitAsync(new OffsetCommitCallback() {
                @Override
                public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception exception) {
                    if (exception != null) {
                        System.err.println("提交错误。。。。");
                    }
                }
            });
        }

    }
}
