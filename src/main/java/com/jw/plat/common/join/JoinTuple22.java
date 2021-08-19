package com.jw.plat.common.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class JoinTuple22 implements JoinFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple3<String,Integer, Integer>> {
    @Override
    public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
        return Tuple3.of(t1.f0, t1.f1, t2.f1);
    }
}
