package com.sd.daoc.messageQueue.kafka;

import com.sd.daoc.messageQueue.kafka.consumer.KafkaObjectReceiver;
import com.sd.daoc.messageQueue.kafka.consumer.KafkaObjectReceiverFactory;
import com.sd.daoc.messageQueue.kafka.handle.IObjectHandle;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroDeserializerConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaObjectReceiverTest {
    private static final Logger logger = LoggerFactory.getLogger(KafkaObjectReceiverTest.class);
    public static void main(String[] args){
        final Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.3.206:9092");
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-payments");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://192.168.3.206:8091");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class);
        props.put("security.protocol", "SASL_PLAINTEXT");
        props.put("sasl.mechanism", "PLAIN");
        props.put(KafkaAvroDeserializerConfig.SPECIFIC_AVRO_READER_CONFIG, true);

        KafkaObjectReceiver receiver = KafkaObjectReceiverFactory.getKafkaObjectReceiverInstance(props, true);
        final String topic = "user";
        receiver.subscriptionTopic(topic, new IObjectHandle<User>() {
            @Override
            public void handle(User object) {
                System.out.println(object.getName() + " " +  object.getId());
            }
        }, 1);
        try {
            Thread.sleep(60 * 1000);
            receiver.stop();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
