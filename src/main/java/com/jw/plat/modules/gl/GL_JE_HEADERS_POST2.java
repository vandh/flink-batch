package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple9;

public class GL_JE_HEADERS_POST2 {
    /**
     * 抽取的列数据：
     * je_header_id
     * je_category
     * je_source
     * period_name
     * name
     * currency_code
     * status
     * actual_flag
     * je_batch_id
     * currency_conversion_rate
     *
     * @param env
     * @return
     */
    private final static String COLMASK = "10001111110001000000000010000000000000001";

    public static DataSet<Tuple10<String, String, String, String, String, String, String, String, String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_HEADERS_POST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        DataSource<Tuple10<String, String, String, String, String, String, String, String, String, String>> input1 =
                csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
        return input1.filter(new FilterFunction<Tuple10<String, String, String, String, String, String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple10<String, String, String, String, String, String, String, String, String, String> t1) throws Exception {
                if (Constants.BATCHDATE == null || Constants.BATCHDATE.trim().equals("")) {
                    return true;
                }
                String zq = t1.f3.trim();
//                return true;
                return zq.indexOf(Constants.BATCHDATE) != -1
                        && t1.f7 != null
                        && "A".equals(t1.f7.trim())
                        && "P".equals(t1.f6.trim())
                        && ("Purchase Invoices".equals(t1.f1.trim()) || "Sales Invoices".equals(t1.f1.trim()))
                        && ("Payables".equals(t1.f2.trim()) || "Receivables".equals(t1.f2.trim()));
            }
        });
    }

}

