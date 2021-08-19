package com.jw.plat.modules.ap;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;

public class AP_XLA_AE_HEADERS {
    /**
     * 抽取的列数据：
     * AE_HEADER_ID  1
     * APPLICATION_ID 2
     * ENTITY_ID  4
     * @param env
     * @return
     */
    private final static String COLMASK = "1101";

    public static DataSet<Tuple3<String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_AE_HEADERS, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class)
         .filter(new FilterFunction<Tuple3<String, String, String>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> t1) throws Exception {
                return "200".equals(t1.f1);
            }
        });
    }

}

