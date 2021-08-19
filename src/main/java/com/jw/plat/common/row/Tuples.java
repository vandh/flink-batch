package com.jw.plat.common.row;

import org.apache.flink.api.java.tuple.*;

public class Tuples {
    public static <T> T comb(Tuple t1, Tuple t2, int i3) {
        return comb(t1, null ,t2, null, i3);
    }
    public static <T> T comb(Tuple t1, int[] pos1, Tuple t2, int i3) {
        return comb(t1, pos1 ,t2, null, i3);
    }
    public static <T> T comb(Tuple t1, Tuple t2, int[] pos2, int i3) {
        return comb(t1, null ,t2, pos2, i3);
    }
    public static <T> T comb(Tuple t1, int[] pos1, Tuple t2, int[] pos2, int i3) {
        if((pos1!=null && pos2!=null && pos1.length+pos2.length!=i3)
            || (pos1==null && pos2==null && t1.getArity()+t2.getArity()!=i3)
            || (pos1==null && pos2!=null && t1.getArity()+pos2.length!=i3)
                || (pos1!=null && pos2==null && t2.getArity()+pos1.length!=i3)
        )
            throw new RuntimeException("comb Tuple has error occured!");

        int i1 = pos1==null || pos1.length==0 ? t1.getArity() : pos1.length;
        //对于左外连接，t2可能为空
        int i2 = i3 - i1;
        Tuple t3 = Tuple.newInstance(i3);
        if(pos1==null || pos1.length<1) {
            for(int i=0; i<i1; i++) {
                t3.setField(t1.getField(i), i);
            }
        } else {
            i1 = pos1.length;
            for(int i=0; i<pos1.length; i++) {
                t3.setField(t1.getField(pos1[i]), i);
            }
        }

        if(pos2==null || pos2.length<1) {
            for(int i=0; i<i2; i++) {
                t3.setField(t2==null ? "" : t2.getField(i), i1+i);
            }
        } else {
            for(int i=0; i<pos2.length; i++) {
                t3.setField(t2==null ? "" : t2.getField(pos2[i]), i1+i);
            }
        }

        return (T)t3;
    }

    public static void main(String[] args) {
//        Tuple2<Integer, Integer> t1 = new Tuple2<>(1,2);
//        Tuple3<Integer,Integer,Integer> t2 = new Tuple3<>(3,4,5);
//        System.out.println(t1);
//        System.out.println(t2);
//        Tuple5<Integer,Integer,Integer,Integer,Integer> t5 = TupleJoin.comb(t1, t2, 5);
//        System.out.println(t5);
//        Tuple4<Integer,Integer,Integer,Integer> t4 = TupleJoin.comb(t1, new int[]{1}, t2,  4);
//        System.out.println(t4);
//        Tuple4<Integer,Integer,Integer,Integer> t42 = TupleJoin.comb(t1, t2, new int[]{0,2},  4);
//        System.out.println(t42);
//        Tuple3<Integer,Integer,Integer> t3 = TupleJoin.comb(t1, new int[]{1}, t2, new int[]{0,2},  3);
//        System.out.println(t3);
//
//        Tuple2<String, String> t11 = new Tuple2<>("a","b");
//        Tuple3<String,String,String> t22 = new Tuple3<>("c","d","e");
//        System.out.println(TupleJoin.comb(t11, new int[]{1}, t22, new int[]{0,2},  3).toString());

        Tuple2<Integer,Integer> t1 = (Tuple2<Integer,Integer>)Tuple.newInstance(2);
        t1.setField(100, 0);
        t1.setField(199, 1);
        System.out.println(t1.f0 + "\t" + t1.f1);
    }
}
