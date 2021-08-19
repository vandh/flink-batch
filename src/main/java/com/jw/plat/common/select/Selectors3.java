package com.jw.plat.common.select;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.types.Row;

public class Selectors3 implements KeySelector<Row, Tuple3<String,String,String>> {
    private int[] pos;
    public Selectors3(int[] pos) {
        this.pos = pos;
    }

    @Override
    public Tuple3<String,String,String> getKey(Row row) throws Exception {
        return Tuple3.of(row.getField(pos[0]).toString(), row.getField(pos[1]).toString(), row.getField(pos[2]).toString());
    }
}
