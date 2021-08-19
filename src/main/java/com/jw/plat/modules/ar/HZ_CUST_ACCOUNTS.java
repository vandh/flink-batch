package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;

public class HZ_CUST_ACCOUNTS {
    /**
     (43,11):           Hca.Cust_Account_Id(+)                      1
     (44,28):           Hp.Party_Id(+) = Hca.Party_Id               2
     (12,11):           Hca.Account_Number 客户账户,                4
     */
    private final static String COLMASK = "1101";

    public static DataSet<Tuple3<String, String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.HZ_CUST_ACCOUNTS, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class);
    }
}