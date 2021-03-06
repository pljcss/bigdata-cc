package com.demo.interceptor;

import org.apache.kafka.clients.producer.ProducerInterceptor;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Map;

/**
 * 计数拦截器
 * @author cs
 * @date 2020/11/1 3:13 下午
 */
public class CounterInterceptor implements ProducerInterceptor<String, String> {

    int success;
    int error;

    @Override
    public void configure(Map<String, ?> configs) {

    }

    /**
     * 直接返回 record
     * @param record
     * @return
     */
    @Override
    public ProducerRecord<String, String> onSend(ProducerRecord<String, String> record) {
        return record;
    }

    @Override
    public void onAcknowledgement(RecordMetadata metadata, Exception exception) {
        if (metadata != null) {
            success++;
        } else {
            error++;
        }
    }

    @Override
    public void close() {
        System.out.println("success "+ success);
        System.out.println("error "+ error);
    }
}
