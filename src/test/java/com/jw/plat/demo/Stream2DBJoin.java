package com.jw.plat.demo;

import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.CoGroupedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.CountTrigger;
import org.apache.flink.util.Collector;

public class Stream2DBJoin {

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

    private static FilterFunction<String> getFilter() {
        return new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                //System.out.println("..."+s);
                return s.split(",")[1].length()==5;
            }
        };
    }

    private static CoGroupFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple3<String,Integer, Integer>> getCoResult() {
        return new CoGroupFunction<Tuple2<String,Integer>, Tuple2<String,Integer>, Tuple3<String,Integer, Integer>>() {
            @Override
            public void coGroup(Iterable<Tuple2<String, Integer>> iterable, Iterable<Tuple2<String, Integer>> iterable1,
                                Collector<Tuple3<String,Integer, Integer>> collector) throws Exception {
                if(iterable.iterator().hasNext() && iterable1.iterator().hasNext()) {
                    Tuple2<String, Integer> t1 = iterable.iterator().next();
                    Tuple2<String, Integer> t2 = iterable1.iterator().next();
                    collector.collect(Tuple3.of(t1.f0, t1.f1, t2.f1));
                }
            }
        };
    }

    public static void main(String[] args) throws Exception {
        String propertiesPath = Stream2DBJoin.class.getResource("/").getPath()+"conf.properties"; //args[0];
        ParameterTool parameterTool = ParameterTool.fromPropertiesFile(propertiesPath);
        String kkPath1 = parameterTool.get("kkPath1");
        String kkPath3 = parameterTool.get("kkPath3");

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        //DataStreamSource<String> fs2 = env.readTextFile(kkPath3);
        DataStream<Tuple2<String,Integer>> input1 = env.readTextFile(kkPath1).filter(getFilter()).map(getMap());
        DataStream<Tuple2<String,Integer>> input2 = env.readTextFile(kkPath3).filter(getFilter()).map(getMap());

        //input1.print();
        //System.out.println("^^^^^^^^^^^^^^^^^^^^^");
        //input2.print();

        //https://www.cnblogs.com/qiu-hua/p/13796777.html
        CoGroupedStreams co1 = input1.coGroup(input2);
        co1.where(getSelector())
                .equalTo(getSelector())
                .window(ProcessingTimeSessionWindows.withGap(Time.seconds(3)))
                //.window(TumblingEventTimeWindows.of(Time.seconds(1)))
                .trigger(CountTrigger.of(1))
                .apply(getCoResult()).print();

        env.execute("simon execute ...");
    }
}
