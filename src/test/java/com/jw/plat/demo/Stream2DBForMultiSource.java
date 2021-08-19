package com.jw.plat.demo;

import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Stream2DBForMultiSource {

    public static void main(String[] args) throws Exception {
        String propertiesPath = Stream2DBForMultiSource.class.getResource("/").getPath()+"conf.properties"; //args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesPath);
        String rawPath = parameterTool.get("rawPath");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);
        DataStreamSource<String> fs1 = env.readTextFile(rawPath+"1");
        DataStreamSource<String> fs2 = env.readTextFile(rawPath+"2");
        DataStreamSource<String> fs3 = env.readTextFile(rawPath+"3");

        fs1.union(fs2,fs3).map(new RowMapFunciton()).addSink(new Flink2JdbcWriter());
        env.execute("开始读取文件...");
    }
}
