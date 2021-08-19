package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;

public class Hz_Parties {
    /**
     (44,11):            And Hp.Party_Id(+) = Hca.Party_Id              1
     (10,11):           Hp.party_number 客户编码,                       2
     (11,11):           Hp.party_name   客户名称,                       3
     */
    private final static String COLMASK = "111";

    public static DataSet<Tuple3<String, String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.HZ_PARTIES, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class);
    }
}