package com.demo.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * 注意一些参数
 * @author cs
 * @date 2020/11/1 1:17 上午
 */
public class ProducerDemo2 {
    public static void main(String[] args) throws InterruptedException {

        // 创建kafka生产者配置信息
        Properties props = new Properties();
        // 指令连接的kafka集群
        props.put("bootstrap.servers", "localhost:9092");
        // ACK应答级别
        props.put("acks", "all");
        // 重试次数
        props.put("retries", "3");
        // 批次大小 16k
        props.put("batch.size", "16384");
        // 等待时间
        props.put("linger.ms", "1");
        // RecordAccumulator缓冲区大小 32M
        props.put("buffer.memory", "33554432");
        // key序列化类
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        // value序列化类
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 创建生产者对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送数据
        for (int i = 0; i < 15; i++) {
            producer.send(new ProducerRecord<>("test2-topic",
//                    Integer.toString(i),
                    "new message" + i));
            Thread.sleep(1*1000);
        }

        // 关闭连接
        producer.close();
    }
}
