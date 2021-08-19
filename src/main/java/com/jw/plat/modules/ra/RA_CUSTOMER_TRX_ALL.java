package com.jw.plat.modules.ra;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;

public class RA_CUSTOMER_TRX_ALL {

    /**
     * 	(39,11):          Rcta.Customer_Trx_Id                          1
     * 	                  Rcta.CREATED_BY                               5               5
     * 	(4,11):           Rcta.Trx_Number 业务系统单据编号,               7
     * 	(44,27):          Rcta.Set_Of_Books_Id                          12->10
     * 	(42,36):          Rcta.Bill_To_Customer_Id                      18
     * (3,11):            Rcta.Org_Id,                                  116
     */
    private final static String COLMASK = "10001010010000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001";

    public static DataSet<Tuple6<String, String, String, String, String,String >> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.RA_CUSTOMER_TRX_ALL, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class,String.class,String.class,String.class);
    }
}