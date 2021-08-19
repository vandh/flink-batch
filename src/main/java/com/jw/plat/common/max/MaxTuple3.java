package com.jw.plat.common.max;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;

public class MaxTuple3 implements ReduceFunction<Tuple3<Integer, Integer, Integer>>{
        @Override
        public Tuple3<Integer, Integer, Integer> reduce(Tuple3<Integer, Integer, Integer> t1, Tuple3<Integer, Integer, Integer> t2) throws Exception {
            System.out.println("......"+t1.f0 +"-"+ t1.f1 +"-"+ t1.f2 +"  :  "+ t2.f0 +"-"+ t2.f1+"-"+ t2.f2);
            return Tuple3.of(t1.f0, Math.max(t1.f1,t2.f1), Math.max(t1.f2, t2.f2));
        }
}
