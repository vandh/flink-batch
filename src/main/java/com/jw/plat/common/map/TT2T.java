package com.jw.plat.common.map;

import com.jw.plat.common.row.Rows;
import com.jw.plat.common.row.Tuples;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.Row;

public class TT2T<T1 extends Tuple,T2 extends Tuple,T3 extends Tuple> implements JoinFunction<T1,T2,T3> {
    private int[] pos1;
    private int[] pos2;
    private int i3;

    public TT2T(int[] pos1, int[] pos2, int i3) {
        this.pos1 = pos1;
        this.pos2 = pos2;
        this.i3 = i3;
    }

    @Override
    public T3 join(T1 t1, T2 t2) throws Exception {
        return Tuples.comb(t1, pos1, t2, pos2, i3);
    }
}
