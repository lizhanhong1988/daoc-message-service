package com.sd.daoc.messageQueue.kafka;

import com.sd.daoc.messageQueue.kafka.producer.KafkaObjectSender;
import com.sd.daoc.messageQueue.kafka.producer.KafkaObjectSenderFactory;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;


public class KafkaObjectSenderFactoryTest {

    public static void main(String[] args) {
        final Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.206:9092");
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 1);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.3.206:8091");


        KafkaObjectSender sender = KafkaObjectSenderFactory.getKafkaObjectSender(props);
        for (int i = 0; i < 1000; i++) {
            User user = new User();
            user.setId(i);
            user.setName("User" + i);
            user.setAge(19 + i);
            //sender.sendObject("user", user.toString(), user);
            sender.sendObject("user", user);
        }
        sender.close();
    }
}
