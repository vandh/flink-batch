package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple10;

public class GL_CODE_COMBINATIONS {
    /**
     *
     (58,35):      GCC.CODE_COMBINATION_ID          1
     gcc.segment1,                                  10
     gcc.segment2,                                  11
     gcc.segment3,                                  12
     gcc.segment4,                                  13
     gcc.segment5,                                  14
     gcc.segment6,                                  15
     gcc.segment7,                                  16
     gcc.segment8,                                  17
     gcc.segment9,                                  18
     */
    private final static String COLMASK = "100000000111111111";

    public static DataSet<Tuple10< String,String,String,String,String, String,String,String,String,String >> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_CODE_COMBINATIONS, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class);
    }
}