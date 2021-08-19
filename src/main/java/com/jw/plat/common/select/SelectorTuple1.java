package com.jw.plat.common.select;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.types.Row;

public class SelectorTuple1 implements KeySelector<Row, Tuple1<String>> {
    private int pos;
    public SelectorTuple1(int pos) {
        this.pos = pos;
    }
    @Override
    public Tuple1<String> getKey(Row row) throws Exception {
        return Tuple1.of(row.getField(pos).toString().trim());
    }
}
