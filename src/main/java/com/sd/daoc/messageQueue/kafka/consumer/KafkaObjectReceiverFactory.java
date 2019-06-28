package com.sd.daoc.messageQueue.kafka.consumer;

import com.sd.daoc.messageQueue.kafka.handle.IObjectHandle;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class KafkaObjectReceiverFactory {
    //private KafkaConsumer<String, T> consumer;

    public static KafkaObjectReceiver getKafkaObjectReceiverInstance(Properties kafkaProperties, boolean isStartFromLatest){
        kafkaProperties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        kafkaProperties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        KafkaConsumer consumer = new KafkaConsumer(kafkaProperties);
        KafkaObjectReceiver receiver = new KafkaObjectReceiver();
        receiver.setConsumer(consumer);
        receiver.setConsumeFromLatest(isStartFromLatest);
        return receiver;
    }

}
