package com.jw.plat.demo;

import com.jw.plat.common.util.MysqlUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Types;

public class Batch2DBJoin {
    public static OutputFormat insertMysql(){
        OutputFormat insertMysql = MysqlUtil.buildMysqlUtil()
                .setQuery(Utils.T_SQL2)
                .setSqlTypes(new int[] {Types.INTEGER, Types.VARCHAR,Types.INTEGER,Types.INTEGER,Types.INTEGER})
                .finish();
        return insertMysql;
    }
    private static MapFunction<String, Tuple2<String,Integer>> getMap() {
        return new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String s) throws Exception {
                String[] t2 = s.split(",");
                //System.out.println("------"+s);
                return Tuple2.of(t2[1], Integer.valueOf(t2[2]));
            }
        };
    }

    private static KeySelector<Tuple2<String,Integer>, String> getSelector() {
        return new KeySelector<Tuple2<String,Integer>, String>() {
            @Override
            public String getKey(Tuple2<String,Integer> t2) throws Exception {
                return t2.f0;
            }
        };
    }
    private static KeySelector<Tuple3<String,Integer,Integer>, String> getSelector2() {
        return new KeySelector<Tuple3<String,Integer,Integer>, String>() {
            @Override
            public String getKey(Tuple3<String, Integer, Integer> t2) throws Exception {
                return t2.f0;
            }
        };
    }

    private static FilterFunction<String> getFilter() {
        return new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                //System.out.println("..."+s);
                return s.split(",")[1].length()==5;
            }
        };
    }

    private static JoinFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple3<String,Integer, Integer>> getJoin() {
        return new JoinFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple3<String,Integer, Integer>>(){
            @Override
            public Tuple3<String, Integer, Integer> join(Tuple2<String, Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple3.of(t1.f0, t1.f1, t2.f1);
            }
        };
    }

    private static JoinFunction<Tuple3<String,Integer, Integer>, Tuple2<String,Integer>, Tuple4<String,Integer, Integer, Integer>> getJoin2() {
        return new JoinFunction<Tuple3<String,Integer,Integer>, Tuple2<String,Integer>, Tuple4<String,Integer, Integer, Integer>>(){
            @Override
            public Tuple4<String, Integer, Integer, Integer> join(Tuple3<String, Integer,Integer> t1, Tuple2<String, Integer> t2) throws Exception {
                return Tuple4.of(t1.f0, t1.f1, t1.f2, t2.f1);
            }
        };
    }

    public static void main(String[] args) throws Exception {
        String propertiesPath = Batch2DBJoin.class.getResource("/").getPath()+"conf.properties"; //args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesPath);
        String kkPath1 = parameterTool.get("kkPath1");
        String kkPath3 = parameterTool.get("kkPath3");
        String kkPath2 = parameterTool.get("kkPath2");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        //DataStreamSource<String> fs2 = env.readTextFile(kkPath3);
        DataSet<Tuple2<String,Integer>> input1 = env.readTextFile(kkPath1).filter(getFilter()).map(getMap());
        DataSet<Tuple2<String,Integer>> input3 = env.readTextFile(kkPath3).filter(getFilter()).map(getMap());
        DataSet<Tuple2<String,Integer>> input2 = env.readTextFile(kkPath2).filter(getFilter()).map(getMap());

        //input1.print();
        //System.out.println("^^^^^^^^^^^^^^^^^^^^^");
        //input2.print();

        //https://www.cnblogs.com/qiu-hua/p/13796777.html
        DataSet<Tuple3<String,Integer, Integer>> input13 = input1.joinWithHuge(input3)
                .where(getSelector())
                .equalTo(getSelector())
                .with(getJoin());
        //input13.print();

        DataSet<Tuple4<String,Integer, Integer, Integer>> input132 = input13.joinWithHuge(input2)
                .where(getSelector2())
                .equalTo(getSelector())
                .with(getJoin2());

        input132.output(insertMysql());
        input132.print();
        //env.execute("simon execute ...");
    }
}
