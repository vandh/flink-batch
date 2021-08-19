package com.jw.plat.modules.approve.consumer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class MessageSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {

    @Override
    public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
        if (value != null && value.contains(",")) {
            String[] parts = value.split(",");
            out.collect(new Tuple2<>(parts[0], Integer.parseInt(parts[1])));
        }
    }
}
