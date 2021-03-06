package com.demo.interceptor;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * @author cs
 * @date 2020/11/1 3:17 下午
 */
public class InterceptorProducer {
    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "localhost:9092");
        props.setProperty("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.setProperty("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        // 添加拦截器
        List<String> list = new ArrayList<>();
        list.add("com.kafkademo.interceptor.TimeInterceptor");
        list.add("com.kafkademo.interceptor.CounterInterceptor");
        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, list);

        // 创建生产者
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // 发送数据
        for (int i = 0; i < 15; i++) {
            producer.send(new ProducerRecord<>("test2-topic",
                    "new message" + i));
//            Thread.sleep(1*100);
        }

        // 关闭连接
        producer.close();
    }
}
