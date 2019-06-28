package com.sd.daoc.messageQueue.kafka.producer;

import com.sd.daoc.messageQueue.kafka.producer.KafkaObjectSender;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaObjectSenderFactory{
    /**
     * @param kafkaProducerProperties Kafka生产者的配置属性
     * @return
     */

    public static KafkaObjectSender getKafkaObjectSender(Properties kafkaProducerProperties) {
        KafkaObjectSender sender = new KafkaObjectSender();
        kafkaProducerProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        kafkaProducerProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        KafkaProducer producer = new KafkaProducer(kafkaProducerProperties);
        sender.setKafkaProducer(producer);
        return sender;
    }

}
