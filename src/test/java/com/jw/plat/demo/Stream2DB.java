package com.jw.plat.demo;

import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Stream2DB {

    public static void main(String[] args) throws Exception {
        String propertiesPath = Stream2DB.class.getResource("/").getPath()+"conf.properties"; //args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesPath);
        String rawPath = parameterTool.get("rawPath");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> fs1 = env.readTextFile(rawPath);


        SingleOutputStreamOperator<Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer>> map1 =
                fs1.filter((value) -> value.length()>5).map(new RowMapFunciton());

        //map.print();
        map1.addSink(new Flink2JdbcWriter());
        env.execute("开始读取文件...");
    }
}
