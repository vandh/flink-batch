package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple6;

public class AR_CASH_RECEIPT_HISTORY_ALL {
    /**
     * (41,11):            Acrha.Cash_Receipt_History_Id                  1
     * (40,34):            Acrha.Cash_Receipt_Id                          2
     * (6,11):             Acrha.gl_date 会计期间,                        9
     */
    private final static String COLMASK = "110000001";

    public static DataSet<Tuple3<String, String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AR_CASH_RECEIPT_HISTORY_ALL, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class);
    }
}