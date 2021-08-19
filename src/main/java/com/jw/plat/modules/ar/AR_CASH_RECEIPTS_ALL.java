package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;

public class AR_CASH_RECEIPTS_ALL {

    /**
     Acrha.Cash_Receipt_Id                            1
     Acra.CREATED_BY                                  5
     Acra.Set_Of_Books_Id                             8
     Acra.Receipt_Number 业务系统单据编号,             14
     Acra.comments 备注                               17
     Acra.Org_Id,                                    64
     */
    private final static String COLMASK = "1000100100000100100000000000000000000000000000000000000000000001";

    public static DataSet<Tuple6<String, String,String, String,String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AR_CASH_RECEIPTS_ALL, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class,String.class, String.class,String.class, String.class);
    }
}