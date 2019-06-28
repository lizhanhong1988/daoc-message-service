package com.sd.daoc.messageQueue.kafka.producer;

import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.Future;

public class KafkaObjectSender<T extends SpecificRecordBase> {
    private static final Logger logger = LoggerFactory.getLogger(KafkaObjectSender.class);
    private KafkaProducer<String, T> kafkaProducer;

    /**
     *
     * @param topic
     * @param key  用于确定写入Kafka分区的分区号
     * @param object
     */
    public void sendObject(String topic, String key, T object) {
        final ProducerRecord<String, T> record = new ProducerRecord(topic, key, object);
        Future<RecordMetadata> meta = kafkaProducer.send(record);
        //("send class[{}] object. {}", object.getClass(), object.toString());
    }

    /**
     *
     * @param topic
     * @param object
     */
    public void sendObject(String topic, T object){
        this.sendObject(topic, object.toString(), object);
    }

    public void sendObject(String topic, Integer key, T object){
        this.sendObject(topic, String.valueOf(key), object);
    }

    /**
     * 结束Kafka写入过程， 并刷新缓冲区
     */
    public void close(){
        if(this.kafkaProducer != null) {
            this.kafkaProducer.flush();
            this.kafkaProducer.close();
        }
    }

    void setKafkaProducer(KafkaProducer<String, T> kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

}