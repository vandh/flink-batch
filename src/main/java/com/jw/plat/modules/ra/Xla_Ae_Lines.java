package com.jw.plat.modules.ra;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple7;

public class Xla_Ae_Lines {
    /**
     (52,30):           Xal.Ae_Header_Id                            1
     (55,11):           Xal.Ae_Line_Num                             2
     (51,32):           Xal.Application_Id                          3
     (58,11):           Xal.CODE_COMBINATION_ID                     4
     (23,11):           Xal.ACCOUNTED_DR 借项,                       13
     (24,11):           Xal.ACCOUNTED_CR 贷项,                        14
     (12,11):           Xal.USSGL_TRANSACTION_CODE 凭证编号,         21
     */
    private final static String COLMASK = "111100000000110000001";

    public static DataSet<Tuple7<String,String,String,String,String,String,String >> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_AE_LINES, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class);
    }
}