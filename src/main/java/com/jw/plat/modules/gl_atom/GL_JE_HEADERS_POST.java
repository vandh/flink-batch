package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple10;

public class GL_JE_HEADERS_POST {
    /**
     * 抽取的列数据：
     * 0:je_header_id     1 0
     * 1:je_category  5
     * 2:je_source    6
     * 3:period_name  7
     * 4:name 8
     * 5:currency_code    9
     * 6:actual_flag      14
     * 7:je_batch_id      25
     * 8:description     35
     * 9:currency_conversion_rate     41
     * @param env
     * @return
     */
    private final static String COLMASK = "10001111100001000000000010000000001000001";

    public static DataSet<Tuple10<String,String,String,String,String,String,String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_HEADERS_POST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        DataSource<Tuple10<String, String, String, String, String,  String, String, String, String, String>> input1 =
                csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
        return input1.filter(new FilterFunction<Tuple10<String, String, String, String, String, String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple10<String, String, String, String, String, String, String, String, String, String> t9) throws Exception {
                return t9.f6.trim()!=null && "A".equals(t9.f6.trim());
            }
        });
    }
}

