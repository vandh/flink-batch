package com.jw.plat.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;

import java.sql.Types;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.jdbc.JDBCOutputFormat;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.types.Row;

public class Batch2DB {
    public static void main1(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer>> dataStream = env
                .addSource(new Flink2JdbcReader());
        // tranfomat
        dataStream.addSink(new Flink2JdbcWriter());
        env.execute("Flink cost DB data to write Database");

    }
    public static void main2(String[] args) throws Exception {
        String propertiesPath = Batch2DB.class.getResource("/").getPath()+"conf.properties"; //args[0];
        //System.out.println(propertiesPath);
        //System.out.println(FlinkReadDbWriterDb.class.getResource("/").getPath());
        //System.out.println(new File(propertiesPath).exists());
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesPath);
        String rawPath = parameterTool.get("rawPath");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = env.readTextFile(rawPath);

        SingleOutputStreamOperator<Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer>> map =
                dataStreamSource.map(new MapFunction<String, Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer>>() {
                    @Override
                    public Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer> map(String s) throws Exception {
                        String[] splits = s.split(",");
                        Integer f1 = 0;
                        try{f1 = Integer.valueOf(splits[0]);}catch(Exception e){}
                        Float f2 = 0f;
                        try{f2 = Float.valueOf(splits[1]);}catch(Exception e){}
                        Integer f3 = 0;
                        try{f3 = Integer.valueOf(splits[2]);}catch(Exception e){}
                        Integer f4 = 0;
                        try{f4 = Integer.valueOf(splits[3]);}catch(Exception e){}
                        Float f5 = 0f;
                        try{f5 = Float.valueOf(splits[4]);}catch(Exception e){}
                        Integer f6 = 0;
                        try{f6 = Integer.valueOf(splits[5]);}catch(Exception e){}
                        Integer f7 = 0;
                        try{f7 = Integer.valueOf(splits[6]);}catch(Exception e){}
                        Integer f8 = 0;
                        try{f8 = Integer.valueOf(splits[7]);}catch(Exception e){}
                        Integer f9 = 0;
                        try{f9 = Integer.valueOf(splits[8]);}catch(Exception e){}
                        Integer f10 = 0;
                        try{f10 = Integer.valueOf(splits[9]);}catch(Exception e){}
                        Integer f11 = 0;
                        try{f11 = Integer.valueOf(splits[10]);}catch(Exception e){}

                        return new Tuple11<>(f1, f2, f3, f4, f5, f6, f7, f8, f9, f10, f11);
                    }
                });

        //map.print();
        //map.addSink(new Flink2JdbcWriter());
    }

    public static OutputFormat insertMysql(){
        OutputFormat insertMysql = JDBCOutputFormat.buildJDBCOutputFormat()
                .setDrivername(Utils.DRIVER)
                .setDBUrl(Utils.URL)
                .setUsername(Utils.USER)
                .setPassword(Utils.PASS)
                .setQuery(Utils.T_SQL)
                .setBatchInterval(1000)
                .setSqlTypes(new int[] {Types.INTEGER,Types.DOUBLE,Types.INTEGER,Types.INTEGER,Types.DOUBLE,
                        Types.INTEGER,Types.INTEGER,Types.INTEGER,Types.INTEGER,Types.INTEGER,Types.INTEGER})
                .finish();
        return insertMysql;
    }

    public static void main(String[] args) throws Exception {
        String propertiesPath = Batch2DB.class.getResource("/").getPath()+"conf.properties"; //args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesPath);
        String rawPath = parameterTool.get("rawPath");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(rawPath);
        DataSet<Row> map = text.map(new MapFunction<String, Row>() {
                    @Override
                    public Row map(String s) throws Exception {
                        String[] splits = s.split(",");
                        Integer f1 = 0;
                        try{f1 = Integer.valueOf(splits[0]);}catch(Exception e){}
                        double f2 = 0d;
                        try{f2 = Double.valueOf(splits[1]);}catch(Exception e){}
                        Integer f3 = 0;
                        try{f3 = Integer.valueOf(splits[2]);}catch(Exception e){}
                        Integer f4 = 0;
                        try{f4 = Integer.valueOf(splits[3]);}catch(Exception e){}
                        double f5 = 0d;
                        try{f5 = Double.valueOf(splits[4]);}catch(Exception e){}
                        Integer f6 = 0;
                        try{f6 = Integer.valueOf(splits[5]);}catch(Exception e){}
                        Integer f7 = 0;
                        try{f7 = Integer.valueOf(splits[6]);}catch(Exception e){}
                        Integer f8 = 0;
                        try{f8 = Integer.valueOf(splits[7]);}catch(Exception e){}
                        Integer f9 = 0;
                        try{f9 = Integer.valueOf(splits[8]);}catch(Exception e){}
                        Integer f10 = 0;
                        try{f10 = Integer.valueOf(splits[9]);}catch(Exception e){}
                        Integer f11 = 0;
                        try{f11 = Integer.valueOf(splits[10]);}catch(Exception e){}

                        Row row = new Row(11);
                        row.setField(0, f1);
                        row.setField(1, f2);
                        row.setField(2, f3);
                        row.setField(3, f4);
                        row.setField(4, f5);
                        row.setField(5, f6);
                        row.setField(6, f7);
                        row.setField(7, f8);
                        row.setField(8, f9);
                        row.setField(9, f10);
                        row.setField(10, f11);

                        return row;
                    }
                });

        text.output(insertMysql());

        env.execute("开始读取文件...");
    }

}
