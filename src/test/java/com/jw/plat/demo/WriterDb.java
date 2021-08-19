package com.jw.plat.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.io.OutputFormat;
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

import java.sql.Types;

public class WriterDb {
    private static int count;
    public static void main(String[] args) throws Exception {
        String propertiesPath = WriterDb.class.getResource("/").getPath()+"conf.properties"; //args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesPath);
        String rawPath = parameterTool.get("rawPath");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSet<String> text = env.readTextFile(rawPath);
        DataSet<Row> map = text.map(new MapFunction<String, Row>() {
                    @Override
                    public Row map(String s) throws Exception {
                        System.out.println(" ***** "+ count++ );
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
        map.print();
        //env.execute("开始读取文件...");
    }

}
