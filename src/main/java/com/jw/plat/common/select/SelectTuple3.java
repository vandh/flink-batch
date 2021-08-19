package com.jw.plat.common.select;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;

public class SelectTuple3 implements KeySelector<Tuple3<String, Integer, Integer>, String>{
    @Override
    public String getKey(Tuple3<String, Integer, Integer> t2) throws Exception {
        return t2.f0;
    }
}
