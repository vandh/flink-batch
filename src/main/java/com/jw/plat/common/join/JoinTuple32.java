package com.jw.plat.common.join;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class JoinTuple32 implements
        JoinFunction<Tuple3<String,Integer,Integer>, Tuple2<String,Integer>, Tuple4<String,Integer, Integer, Integer>> {
    @Override
    public Tuple4<String, Integer, Integer, Integer> join(Tuple3<String, Integer,Integer> t1, Tuple2<String, Integer> t2) throws Exception {
        return Tuple4.of(t1.f0, t1.f1, t1.f2, t2.f1);
    }
}
