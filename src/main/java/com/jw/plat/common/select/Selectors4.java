package com.jw.plat.common.select;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.types.Row;

public class Selectors4 implements KeySelector<Row, Tuple4<String,String,String,String>> {
    private int[] pos;
    public Selectors4(int[] pos) {
        this.pos = pos;
    }

    @Override
    public Tuple4<String,String,String,String> getKey(Row row) throws Exception {
        return Tuple4.of(row.getField(pos[0]).toString(), row.getField(pos[1]).toString(), row.getField(pos[2]).toString(),row.getField(pos[2]).toString());
    }
}
