package com.jw.plat.modules.ap;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;

public class AP_GL_IMPORT_REFERENCES {
    /**
     * 抽取的列数据：
     * batch_id
     * batch_name
     * @param env
     * @return
     */
    private final static String COLMASK = "1";

    public static DataSet<Tuple1<String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_GL_IMPORT_REFERENCES, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.LF)
                        .ignoreInvalidLines()
                        .types(String.class);
    }

}

