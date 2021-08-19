package com.jw.plat.modules.approve.producer;

import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.kafka.clients.producer.*;

import java.util.Properties;

public class KafkaProducerUtil {
    public static final String TOPIC="test";
    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.10.220:9092");
        props.setProperty("transaction.timeout.ms","100000");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        //FlinkKafkaProducer<String> producer = new FlinkKafkaProducer<>(TOPIC, new ProducerStringSerializationSchema(TOPIC), props, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
        Producer<String, String> producer = new KafkaProducer<>(props);
        int totalMessageCount = 10000;
        for (int i = 0; i < totalMessageCount; i++) {
            String value = String.format("%s,%d","c1", 1);
            producer.send(new ProducerRecord<>(TOPIC, value), new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    if (exception != null) {
                        System.out.println("Failed to send message with exception " + exception);
                    }
                }
            });
            Thread.sleep(1000L);
        }
        producer.close();
    }

    private static long currentMemSize() {
        return MemoryUsageExtrator.currentFreeMemorySizeInBytes();
    }
}
