package com.sd.daoc.messageQueue.kafka.handle;

import org.apache.avro.specific.SpecificRecordBase;

public interface IObjectHandle<T extends SpecificRecordBase> {
    void handle(T object);
}
