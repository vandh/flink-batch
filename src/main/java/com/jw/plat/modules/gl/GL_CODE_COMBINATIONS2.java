package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple12;

public class GL_CODE_COMBINATIONS2 {
    /**
     * 抽取的列数据：
     * code_combination_id
     * chart_of_accounts_id
     * summary_flag
     * segment1
     * segment2
     * segment3
     * segment4
     * segment5
     * segment6
     * segment7
     * segment8
     * segment9
     * @param env
     * @return
     */
    private final static String COLMASK = "100100001111111111";

    public static DataSet<Tuple12<String,String,String,String,String,String,String,String,String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_CODE_COMBINATIONS, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class)
                .filter(new FilterFunction<Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>>() {
                    @Override
                    public boolean filter(Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> t) throws Exception {

                        return "N".equals(t.f2.trim());
//                        return "43070101030801".equals(t.f6.trim());
                    }
                });
    }
}

