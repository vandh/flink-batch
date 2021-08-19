package com.jw.plat.common.map;

import com.jw.plat.common.row.Rows;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.Row;

public class TT2R<T1 extends Tuple,T2 extends Tuple> implements JoinFunction<T1,T2,Row> {
    private int[] pos1;
    private int[] pos2;

    public TT2R(int[] pos1, int[] pos2, int i3) {
        this.pos1 = pos1;
        this.pos2 = pos2;
    }
    @Override
    public Row join(T1 t1, T2 t2) throws Exception {
        return Rows.create(t1, pos1, t2, pos2);
    }
}
