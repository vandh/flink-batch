package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class Xla_Events {
    /**
     *               Xe.Event_Id                    1
     (47,32):        Xe.Application_Id              2
     (48,27):        Xe.Entity_Id                   5
     */
    private final static String COLMASK = "11001";

    public static DataSet<Tuple3< String,String,String >> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.XLA_EVENTS, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class);
    }
}