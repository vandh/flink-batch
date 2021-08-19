package com.jw.plat.demo;

import com.jw.plat.common.util.Constants;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class BatchJoinMulti {

    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        FilterOperator<Tuple2<String,String>> ds1 = env.readCsvFile("d:/flink/kk1")
                .includeFields("11")
                .lineDelimiter(Constants.HH)
                .fieldDelimiter(Constants.DH)
                .ignoreInvalidLines()
                .types(String.class,String.class)
                .filter(new FilterFunction<Tuple2<String, String>>() {
                    @Override
                    public boolean filter(Tuple2<String, String> t2) throws Exception {
                        return true;
                    }
                });

        FilterOperator<Tuple3<String, String,String>> ds2 = env.readCsvFile("d:/flink/kk2")
                .includeFields("111")
                .lineDelimiter(Constants.HH)
                .fieldDelimiter(Constants.DH)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class)
                .filter(new FilterFunction<Tuple3<String, String,String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String,String> t2) throws Exception {
                        return true;
                    }
                });
        FilterOperator<Tuple4<String,String,String,String>> ds3 = env.readCsvFile("d:/flink/kk3")
                .includeFields("1111")
                .lineDelimiter(Constants.HH)
                .fieldDelimiter(Constants.DH)
                .ignoreInvalidLines()
                .types(String.class,String.class, String.class,String.class)
                .filter(new FilterFunction<Tuple4<String,String,String,String>>() {
                    @Override
                    public boolean filter(Tuple4<String,String,String,String> t2) throws Exception {
                        return true;
                    }
                });
//        ds1.print();
//        ds2.print();
//        ds3.print();

        ds1.joinWithTiny(ds2)
                .where(0,1)
                .equalTo(0,1)
                .with(new JoinFunction<Tuple2<String, String>, Tuple3<String,String, String>, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> join(Tuple2<String, String> t1, Tuple3<String,String, String> t2) throws Exception {
                        return Tuple3.of(t1.f0, t1.f1, t2.f2);
                    }
                })
                .joinWithTiny(ds3)
                .where(0)
                .equalTo(0)
                .with(new JoinFunction<Tuple3<String, String, String>, Tuple4<String,String,String, String>, Tuple4<String, String, String, String>>() {
                    @Override
                    public Tuple4<String, String, String, String> join(Tuple3<String, String, String> t1, Tuple4<String,String,String, String> t2) throws Exception {
                        return Tuple4.of(t1.f0, t1.f1, t1.f2, t2.f3);
                    }
                })
                .print();

//        env.execute();
    }


}
