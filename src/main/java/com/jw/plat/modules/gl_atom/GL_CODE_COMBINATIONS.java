package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple12;

public class GL_CODE_COMBINATIONS {
    /**
     * 抽取的列数据：
     * 0：code_combination_id
     * 1：chart_of_accounts_id
     * 2：summary_flag
     * 3：segment1
     * 4：segment2
     * 5：segment3
     * 6：segment4
     * 7：segment5
     * 8：segment6
     * 9：segment7
     * 10：segment8
     * 11：segment9
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
                        .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class);
//                .filter(new FilterFunction<Tuple12<String, String, String, String, String, String, String, String, String, String, String, String>>() {
//                    @Override
//                    public boolean filter(Tuple12<String, String, String, String, String, String, String, String, String, String, String, String> t12) throws Exception {
//                        String summary_flag = t12.f2.trim();
//                        if(summary_flag!=null && summary_flag.equals("N")) return true;
//                        return false;
//                    }
//                }
//                )

    }
}

