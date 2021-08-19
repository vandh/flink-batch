package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple4;

public class QGL_SEQUENCE_VALUE {
    /**
     * 抽取的列数据：
     *0: je_header_id
     *1: sequence_value
     *2: last_update_date
     *3: ods_creation_date
     * @param env
     * @return
     */
    private final static String COLMASK = "1111";

    public static DataSet<Tuple4<String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.QGL_SEQUENCE_VALUE, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class,String.class);
    }
}

