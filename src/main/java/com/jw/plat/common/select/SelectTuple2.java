package com.jw.plat.common.select;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;

public class SelectTuple2 implements KeySelector<Tuple2<String,Integer>, String>{
    @Override
    public String getKey(Tuple2<String,Integer> t2) throws Exception {
        return t2.f0;
    }
}
