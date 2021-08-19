package com.jw.plat.modules.ap;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple3;

public class AP_GL_IMPORT_REFERENCES2 {
    /**
     * 抽取的列数据：
     * je_header_id 3
     * je_line_num 4
     * reference_5
     *
     * @param env
     * @return
     */
    private final static String COLMASK = "0011000000000100000000000000000000";

    public static DataSet<Tuple3<String, String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_GL_IMPORT_REFERENCES, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class).filter(
                        new FilterFunction<Tuple3<String, String, String>>() {
                                    @Override
                                    public boolean filter(Tuple3<String, String, String> stringTuple1) throws Exception {
                                        return true;
                                    }
                                });
    }

}

