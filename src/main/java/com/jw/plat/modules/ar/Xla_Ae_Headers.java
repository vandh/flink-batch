package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple4;

public class Xla_Ae_Headers {
    /**
     * 	(8,11):             Xah.Ae_Header_Id Je_Header_Id,                              1
     * 	(52,11):            And Xah.Application_Id = Xal.Application_Id                  2
     * 	(51,25):            And Xe.Event_Id = Xah.Event_Id                              5
     * 	(7,11):             Xah.Ae_batch_id（upg_batch_id） Je_batch_id,                  66
     *
     *
     (6,11):           Xah.Ae_batch_id Je_batch_id,         66
     (7,11):           Xah.Ae_Header_Id Je_Header_Id,       1
     (49,31):          Xah.Application_Id                   2
     (50,25):          Xah.Event_Id                         5
     */
    private final static String COLMASK = "110010000000000000000000000000000000000000000000000000000000000001";

    public static DataSet<Tuple4< String,String,String,String >> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_AE_HEADERS, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class,String.class);
    }
}