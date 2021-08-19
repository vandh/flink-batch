package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class Ar_Distributions_All {
    /**
     * 	(58,46):       Ada.Line_Id                                      1
     * 	(41,43):       Ada.Source_Id                                    2
     * 	(42,11):       Ada.Source_Table in ('CRH','RA','MCD')           3
     * 	(43,36):       Ada.Third_Party_Id                               27
     */
    private final static String COLMASK = "111000000000000000000000001";

    public static DataSet<Tuple4<String, String, String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AR_DISTRIBUTIONS_ALL, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class, String.class)
            .filter(new FilterFunction<Tuple4<String, String, String, String>>() {
            @Override
            public boolean filter(Tuple4<String, String, String, String> t1) throws Exception {
                return "CRH,RA,MCD".indexOf(t1.f2.trim())!=-1;
            }
        });
    }
}