package com.jw.plat.common.row;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.types.Row;

import java.util.List;

public class Rows {
    public static Row create(Tuple t1, int[] pos1, Tuple t2, int[] pos2) {
        int i1 = (pos1!=null && pos1.length>0) ? pos1.length : t1.getArity();
        int i2 = (pos2!=null && pos2.length>0) ? pos2.length : t2.getArity();

        Row row = new Row(i1+i2);
        if(pos1==null || pos1.length<1) {
            for(int i=0; i<i1; i++) {
                row.setField(i, t1.getField(i));
            }
        } else {
            for(int i=0; i<pos1.length; i++) {
                row.setField(i, t1.getField(pos1[i]));
            }
        }

        if(pos2==null || pos2.length<1) {
            for(int i=0; i<i2; i++) {
                row.setField(i1+i, t2.getField(i));
            }
        } else {
            for(int i=0; i<pos2.length; i++) {
                row.setField(i1+i, t2.getField(pos2[i]));
            }
        }

        return row;
    }

    public static Row create(Tuple t1, int[] pos1, Tuple t2) {
        return create(t1, pos1, t2, null);
    }
    public static Row create(Tuple t1, Tuple t2, int[] pos2) {
        return create(t1, null, t2, pos2);
    }
    public static Row create(Tuple t1, Tuple t2) {
        return create(t1, null, t2, null);
    }

    public static Row create(Row t1, Tuple t2) {
        return create(t1, null, t2, null);
    }

    public static Row create(Row t1, int[] pos1, Tuple t2) {
        return create(t1, pos1, t2, null);
    }

    public static Row create(Row t1, Tuple t2, int[] pos2) {
        return create(t1, null, t2, pos2);
    }

    public static Row create(Row t1, int[] pos1, Tuple t2, int[] pos2) {
        int i1 = (pos1!=null && pos1.length>0) ? pos1.length : t1.getArity();
        int i2 = (pos2!=null && pos2.length>0) ? pos2.length : t2.getArity();
        Row row = new Row(i1+i2);

        if(pos1==null || pos1.length<1) {
            for(int i=0; i<i1; i++) {
                row.setField(i, t1.getField(i));
            }
        } else {
            for(int i=0; i<pos1.length; i++) {
                row.setField(i, t1.getField(pos1[i]));
            }
        }

        if(pos2==null || pos2.length<1) {
            for(int i=0; i<i2; i++) {
                row.setField(i1+i, t2.getField(i));
            }
        } else {
            for(int i=0; i<pos2.length; i++) {
                row.setField(i1+i, t2.getField(pos2[i]));
            }
        }


        return row;
    }
    //创建的长度    row   需要的数据索引数组
    public static Row sort(Row t1, int[] array) {
        Row row = new Row(array.length);
        for (int i = 0; i < array.length; i++) {
            row.setField(i, t1.getField(i));
        }
        return  row;
    }
}
