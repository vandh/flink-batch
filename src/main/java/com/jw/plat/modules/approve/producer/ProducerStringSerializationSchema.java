package com.jw.plat.modules.approve.producer;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;
import java.nio.charset.StandardCharsets;

public class ProducerStringSerializationSchema implements KafkaSerializationSchema<Tuple2<String, Integer>> {

    private String topic;

    public ProducerStringSerializationSchema(String topic) {
        super();
        this.topic = topic;
    }

    @Override
    public ProducerRecord<byte[], byte[]> serialize(Tuple2<String, Integer> t2, @Nullable Long aLong) {
        return new ProducerRecord<byte[], byte[]>(topic, t2.f0.getBytes(StandardCharsets.UTF_8));
    }
}
