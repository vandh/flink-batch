package com.jw.plat.dealDateController;

import com.jw.plat.common.row.Tuples;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.core.fs.FileSystem;
import org.junit.Test;

import java.util.logging.Logger;


/**
 * @Author lijun
 * @Date 2021/7/5 16:02
 * @Version 1.0
 */
public class testFlink {

    public static void main(String[] args) throws Exception {
        Logger logger = Logger.getLogger("testFlink.class");
        logger.info("ddddd");
        System.out.println("日志开始================");
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSet<Tuple5<String, String, String, String, String> > dataSet;
        dataSet=demo1proc(env).join(demo2proc(env))
                        .where(1)
                        .equalTo(1)
                        .with(new JoinFunction<Tuple2<String, String>,
                                Tuple3<String, String, String>,
                                Tuple5<String, String, String, String, String>>() {
                            @Override
                            public Tuple5<String, String, String, String, String> join(
                                    Tuple2<String, String> t1,
                                    Tuple3<String, String, String> t2) throws Exception {
                                return Tuples.comb(t1, t2, 5);
                            }
                        });
        dataSet.print();

        dataSet.map(new MapFunction<Tuple5<String, String, String, String, String>, String>() {
            @Override
            public String map(Tuple5<String, String, String, String, String> stringTuple5) throws Exception {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < stringTuple5.getArity(); i++)
                    if (i == stringTuple5.getArity() - 1) {
                        sb.append((String) stringTuple5.getField(i));
                        System.out.println(sb.toString());
                    } else {
                        sb.append((String) stringTuple5.getField(i)).append("\u0007");
                        System.out.println(sb.toString());
                    }
//                        sb.append(stringStringTuple2.getField(i)).append("\u0007");
                return sb.toString();

            }
        }).writeAsText("E:\\testData\\flinkData\\testFlink\\flink-2", FileSystem.WriteMode.OVERWRITE);

        env.execute();
        System.out.println("error================");
    }


    @Test
    public  void demo1() throws Exception {
        System.out.println("日志开始================");
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataSet<Tuple2<String, String>> dataSet = demo1proc(env);
        dataSet.map(new MapFunction<Tuple2<String, String>, String>() {
            @Override
            public String map(Tuple2<String, String> stringStringTuple2) throws Exception {
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < stringStringTuple2.getArity(); i++)
                    if (i == stringStringTuple2.getArity() - 1) {
                        sb.append((String) stringStringTuple2.getField(i));
                        System.out.println(sb.toString());
                    } else {
                        sb.append((String) stringStringTuple2.getField(i)).append("\u0007");
                        System.out.println(sb.toString());
                    }
//                        sb.append(stringStringTuple2.getField(i)).append("\u0007");
                return sb.toString();

            }
        }).writeAsText("E:\\testData\\flinkData\\testFlink\\flink-1", FileSystem.WriteMode.OVERWRITE);
        env.execute();

    }



    /**
     * 抽取的列数据：
     * batch_id
     * batch_name
     */
    private final static String demo1COLMASK = "11";

    public static DataSet<Tuple2<String, String>> demo1proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile("E:\\testData\\flinkData\\testFlink\\demo1.txt");
        csvReader.setCharset("UTF-8");
        return csvReader.includeFields(demo1COLMASK)
                .lineDelimiter("\r\n")  //行分隔符
                .fieldDelimiter(",")//字段分隔符
                .ignoreInvalidLines()
                .types(String.class, String.class);
    }


    /**
     * 抽取的列数据：
     * batch_id
     * batch_name
     */
    private final static String demo2COLMASK = "111";

    public static DataSet<Tuple3<String, String, String>> demo2proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile("E:\\testData\\flinkData\\testFlink\\demo2.txt");
        csvReader.setCharset("UTF-8");
        return csvReader.includeFields(demo2COLMASK)
                .lineDelimiter("\r\n")  //行分隔符
                .fieldDelimiter(",")//字段分隔符
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class);
    }
}
