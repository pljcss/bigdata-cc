package com.demo.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;

/**
 * 自定义分区器
 *
 * @author cs
 * @date 2020/11/1 12:39 下午
 */
public class PartitionerProducerDemo {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 指定自定义分区器
        props.put(ProducerConfig.PARTITIONER_CLASS_CONFIG, "com.kafkademo.producer.MyPartitioner");

        // 创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送数据
        producer.send(new ProducerRecord<>("test2-topic",
                "key333",
                "new message"), new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception e) {
                if (e == null) {
                    System.out.println(metadata.partition() + " : " + metadata.offset());
                } else {
                    e.printStackTrace();
                }
            }
        });

        // 关闭
        producer.close();
    }
}


