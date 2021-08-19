package com.jw.plat.modules.ap;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple7;

public class AP_XLA_AE_LINES {
    /**
     * 抽取的列数据：
     * AE_HEADER_ID  1
     * AE_LINE_NUM   2
     * APPLICATION_ID  3
     * CODE_COMBINATION_ID  4
     * ACCOUNTED_DR  13
     * ACCOUNTED_CR  14
     * USSGL_TRANSACTION_CODE  21
     *
     * @param env
     * @return
     */
    private final static String COLMASK = "111100000000110000001";

    public static DataSet<Tuple7<String, String, String, String, String, String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_AE_LINES, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class);

    }
}


