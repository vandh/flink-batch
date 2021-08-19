package com.jw.plat.modules.ap;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple5;

/**
 * AP_XLA_DIS_LINKS：AE_LINE_NUM不同，所以开始会有重复记录
 */
public class AP_XLA_DIS_LINKS {
    /**
     * 抽取的列数据：
     * APPLICATION_ID   1
     * AE_HEADER_ID  3
     * AE_LINE_NUM  4
     * SOURCE_DISTRIBUTION_TYPE  5
     * SOURCE_DISTRIBUTION_ID_NUM_1  11
     * @param env
     * @return
     */
    private final static String COLMASK = "10111000001";

    public static DataSet<Tuple5<String,String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_DIS_LINKS, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class,String.class,String.class)
                .filter(
                        new FilterFunction<Tuple5<String, String, String, String, String>>() {
                            @Override
                            public boolean filter(Tuple5<String, String, String, String, String> t1) throws Exception {
                                return "AP_INV_DIST".equals(t1.f3);
                            }
                        }
                );
    }

}

