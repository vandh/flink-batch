package com.jw.plat.modules.approve.consumer;

import com.jw.plat.common.select.SelectTuple2;
import com.jw.plat.modules.approve.producer.KafkaProducerUtil;
import com.jw.plat.modules.approve.producer.ProducerStringSerializationSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.Properties;

public class KafkaSource {
    public static void main(String[] args) throws  Exception{
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // exactly-once 语义保证整个应用内端到端的数据一致性
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 开启检查点并指定检查点时间间隔为5s
        env.enableCheckpointing(5000); // checkpoint every 5000 msecs
        // 设置StateBackend，并指定状态数据存储位置
        env.setStateBackend(new FsStateBackend("file:///D:/Temp/checkpoint/flink/KafkaSource"));
        env.setParallelism(1);

        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "192.168.10.220:9092");
        props.setProperty("group.id", "flink-group");
        props.setProperty("log.file", "fk");
        props.setProperty("web.log.path", ".");
//        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
//        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        FlinkKafkaConsumer<String> consumer =
                new FlinkKafkaConsumer<>(KafkaProducerUtil.TOPIC, new SimpleStringSchema(), props);
        consumer.setStartFromLatest(); // 指定从最新offset开始消费
        DataStream<Tuple2<String, Integer>> dataStream = env.addSource(consumer)
                .flatMap(new MessageSplitter())
                .keyBy(new SelectTuple2())
                .countWindow(3)
                .sum(1);
        dataStream.print();

        Properties p2 = new Properties();
        p2.put("bootstrap.servers", "192.168.10.220:9092");
        p2.setProperty("transaction.timeout.ms","100000");
        p2.put("acks", "all");
        p2.put("retries", 2);
        p2.put("batch.size", 16384);
        p2.put("linger.ms", 1);
        p2.put("buffer.memory", 33554432);

        FlinkKafkaProducer<Tuple2<String, Integer>> producer = new FlinkKafkaProducer<>(
                "hello",
                new ProducerStringSerializationSchema("hello"),
                p2,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE);

        dataStream.addSink(producer).name("flink-connectors-kafka");

        // execute program
        env.execute("Flink Streaming—————KafkaSource");
    }
}
