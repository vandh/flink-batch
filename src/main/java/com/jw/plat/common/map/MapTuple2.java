package com.jw.plat.common.map;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;

public class MapTuple2 implements MapFunction<String, Tuple2<String,Integer>>{
    @Override
    public Tuple2<String,Integer> map(String s) throws Exception {
        String[] t2 = s.split(",");
        //System.out.println("------"+s);
        return Tuple2.of(t2[1], Integer.valueOf(t2[2]));
    }
}
