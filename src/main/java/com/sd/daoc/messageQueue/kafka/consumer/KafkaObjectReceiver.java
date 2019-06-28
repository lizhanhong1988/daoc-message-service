package com.sd.daoc.messageQueue.kafka.consumer;

import com.sd.daoc.messageQueue.kafka.handle.IObjectHandle;
import org.apache.avro.specific.SpecificRecordBase;
import org.apache.kafka.clients.consumer.*;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;


public class KafkaObjectReceiver<T extends SpecificRecordBase> implements Runnable{

    private static final Logger logger = LoggerFactory.getLogger(KafkaObjectReceiver.class);
    private KafkaConsumer<String, T> consumer;
    private IObjectHandle handler;
    private AtomicBoolean isStop = new AtomicBoolean(false);
    private String topic;
    private int executorNum;
    private ExecutorService executorService;
    private boolean isConsumeFromLatest = false;

    public void subscriptionTopic(String topic, IObjectHandle<T> handler, int executorNum){
        this.handler = handler;
        this.topic = topic;
        this.executorNum = executorNum;

        executorService = Executors.newFixedThreadPool(executorNum);
        executorService.submit(this);
    }

    private void startConsume(){
        SourceRebalanceListener sourceRebalanceListener = new SourceRebalanceListener();
        consumer.subscribe(Collections.singletonList(topic), sourceRebalanceListener);
        while(!isStop.get()){
            Set<TopicPartition> tps = null;
            try {
                final ConsumerRecords<String, T> records = consumer.poll(2000L);
                for (final ConsumerRecord<String, T> record : records) {
                    long offset = record.offset();
                    this.handler.handle(record.value());
                    final TopicPartition tp = new TopicPartition(this.topic, record.partition());
                    consumer.commitSync(Collections.singletonMap(tp, new OffsetAndMetadata(record.offset() + 1)));
                }
            }catch (SerializationException se){
                logger.error("Topic {} schema is error, {}", this.topic, se.getMessage());
                consumer.seekToEnd(tps);
            }catch (WakeupException ex){
                logger.info("Kafka consumer is stopped.");
            }
        }
    }

    public void stop(){
        logger.info("Kafka consume receive shutdown cmd.");
        this.isStop.set(true);
        this.consumer.wakeup();
        executorService.shutdown();
    }

    @Override
    public void run() {
        this.startConsume();
    }

    void setConsumer(KafkaConsumer<String, T> consumer) {
        this.consumer = consumer;
    }

    public void setHandler(IObjectHandle handler) {
        this.handler = handler;
    }

    public void setConsumeFromLatest(boolean consumeFromLatest) {
        isConsumeFromLatest = consumeFromLatest;
    }

    class SourceRebalanceListener implements ConsumerRebalanceListener {
        private HashSet<Integer> topicPartitionSet;

        public SourceRebalanceListener() {
            this.topicPartitionSet = new HashSet<Integer>();
        }

        // Set a flag that a rebalance has occurred. Then commit already read events
        // to kafka.
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                topicPartitionSet.contains(partition.partition());
                topicPartitionSet.remove(partition.partition());
                logger.info("{} Partition {} is revoked.", partition.topic() , partition.partition());
            }
        }

        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
            for (TopicPartition partition : partitions) {
                if(isConsumeFromLatest){
                    // 重置Offset
                    logger.info("Kafka will consume from latest offset.");
                    consumer.seekToEnd(partitions);
                    try{
                        consumer.commitAsync();
                    }catch(Exception e){

                    }
                }
                topicPartitionSet.add(partition.partition());
                logger.info("{} Partition {} is assigned.", partition.topic(), partition.partition());
            }
            if(isConsumeFromLatest){
                isConsumeFromLatest = false;
            }
        }
    }
}
