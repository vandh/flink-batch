package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple3;

public class QGL_APPROVE {
    /**
     * 抽取的列数据：
     * je_header_id
     * post_person
     * post_date
     * @param env
     * @return
     */
    private final static String COLMASK = "111";

    public static DataSet<Tuple3<String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.QGL_APPROVE, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class);
    }

}

