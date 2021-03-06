package com.demo.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 时间拦截器，在value前面加上时间
 * @author cs
 * @date 2020/11/1 3:06 下午
 */
public class TimeInterceptor implements ProducerInterceptor<String, String> {

    @Override
    public void configure(Map<String, ?> configs) {

    }

    /**
     * 逻辑。。
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        // 1、取出数据
        String oldValue = record.value();
        // 2、添加时间
        String newValue = System.currentTimeMillis() + "," + oldValue;
        // 3、创建新的ProducerRecord对象并返回
        return new ProducerRecord<>(record.topic(), record.partition(), record.key(), newValue);
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {

    }

    @Override
    public void close() {

    }

}
