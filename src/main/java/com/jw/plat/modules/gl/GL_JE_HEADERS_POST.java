package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple9;

public class GL_JE_HEADERS_POST {
    /**
     * 抽取的列数据：
     * je_header_id     1
     * je_category  5
     * je_source    6
     * period_name  7
     * name 8
     * currency_code    9
     * actual_flag      14
     * je_batch_id      25
     * currency_conversion_rate     41
     * @param env
     * @return
     */
    private final static String COLMASK = "10001111100001000000000010000000000000001";

    public static DataSet<Tuple9<String,String,String,String,String,String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_HEADERS_POST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        DataSource<Tuple9<String, String, String, String, String, String, String, String, String>> input1 =
                csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
        return input1.filter(new FilterFunction<Tuple9<String, String, String, String, String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple9<String, String, String, String, String, String, String, String, String> t1) throws Exception {
                if (Constants.BATCHDATE == null || Constants.BATCHDATE.trim().equals("")) return true;
                String zq = t1.f3.trim();
//                return true;
                return zq.indexOf(Constants.BATCHDATE) != -1&& t1.f6!=null && "A".equals(t1.f6.trim());
                /*return t1.f7.equals("8866616")*/

            }
        });
    }
}

