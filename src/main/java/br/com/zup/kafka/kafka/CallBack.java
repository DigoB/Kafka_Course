package br.com.zup.kafka.kafka;

import org.apache.kafka.clients.producer.RecordMetadata;

public interface CallBack {
    void onCompletion(RecordMetadata var1, Exception var2);
    
}
