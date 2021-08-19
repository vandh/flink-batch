package com.jw.plat.modules.ra;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;

public class RA_CUSTOMER_TRX_LINES_ALL {
    /**
     (41,42):           Rctla.Customer_Trx_Line_Id(+)         1
     (40,37):           Rctla.Customer_Trx_Id(+)              7
     (26,11):           Rctla.description 摘要                12
     */
    private final static String COLMASK = "100000100001";

    public static DataSet<Tuple3< String, String,String >> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.RA_CUSTOMER_TRX_LINES_ALL, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class);
    }
}