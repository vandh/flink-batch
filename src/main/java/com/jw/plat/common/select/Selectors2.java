package com.jw.plat.common.select;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

public class Selectors2 implements KeySelector<Row, Tuple2<String,String>> {
    private int[] pos;
    public Selectors2(int[] pos) {
        this.pos = pos;
    }

    @Override
    public Tuple2<String,String> getKey(Row row) throws Exception {
        return Tuple2.of(row.getField(pos[0]).toString(), row.getField(pos[1]).toString());
    }
}
