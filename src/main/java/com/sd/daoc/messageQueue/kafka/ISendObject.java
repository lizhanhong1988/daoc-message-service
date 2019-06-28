package com.sd.daoc.messageQueue.kafka;

import org.apache.avro.specific.SpecificRecordBase;

public interface ISendObject<T extends SpecificRecordBase> {
    void sendObject(String topic, String key, T object, Class clazz);

    void sendObject(String topic, T object, Class clazz);
}
