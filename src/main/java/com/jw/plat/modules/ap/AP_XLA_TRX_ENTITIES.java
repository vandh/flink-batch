package com.jw.plat.modules.ap;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;

public class AP_XLA_TRX_ENTITIES {
    /**
     * 抽取的列数据：
     * ENTITY_ID        1
     * ENTITY_CODE      4
     * SOURCE_ID_INT_1  10
     * SECURITY_ID_INT_1  12
     * @param env
     * @return
     */
    private final static String COLMASK = "100100000101";

    public static DataSet<Tuple4<String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_TRX_ENTITIES, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class,String.class)
                .filter(new FilterFunction<Tuple4<String, String, String, String>>() {
                    @Override
                    public boolean filter(Tuple4<String, String, String, String> t1) throws Exception {
                        return "AP_INVOICES,AP_INVOICE_DISTRIBUTIONS".indexOf(t1.f1)!=-1;
                    }
                });
    }

}

