package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.util.Collector;

public class FND_USER {
    /**
     * 抽取的列数据：
     * user_id
     * user_name
     * last_update_date
     * @param env
     * @return
     */
    private final static String COLMASK = "111";

    public static DataSet<Tuple3<String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.FND_USER, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class)
                .filter(new FilterFunction<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                        return true;
                    }
                })
                /*.flatMap(new FlatMapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
                    @Override
                    public void flatMap(Tuple3<String, String, String> stringStringStringTuple3, Collector<Tuple3<String, String, String>> collector) throws Exception {

                    }
                })*/

               /*
               //业务处理
               .setParallelism(10).map(new MapFunction<Tuple3<String, String, String>, Tuple3<String, String, String>>() {
                    @Override
                    public Tuple3<String, String, String> map(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                        String f0 = stringStringStringTuple3.f0;
                        return null;
                    }
                })*/;
    }

}

