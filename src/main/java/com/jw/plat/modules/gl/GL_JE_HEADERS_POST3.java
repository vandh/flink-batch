package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;

public class GL_JE_HEADERS_POST3 {
    /**
     * 抽取的列数据：
     * je_header_id
     * ledger_id
     * je_category
     * je_source
     * period_name
     * name
     * currency_code
     * status
     * actual_flag
     * DEFAULT_EFFECTIVE_DATE
     * je_batch_id
     * currency_conversion_rate
     *
     * @param env
     * @return
     */
    private final static String COLMASK = "10011111110001100000000010000000000000001";

    public static DataSet<Tuple11<String, String,String, String, String, String, String, String, String, String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_HEADERS_POST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        DataSource<Tuple11<String,String, String, String, String, String, String, String, String, String, String>> input1 =
                csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class, String.class);
        return input1.filter(new FilterFunction<Tuple11<String, String,String, String, String, String, String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple11<String,String, String, String, String, String, String, String, String, String, String> t1) throws Exception {
                if (Constants.BATCHDATE == null || Constants.BATCHDATE.trim().equals("")) {
                    return true;
                }
                String zq = t1.f4.trim();
//                return true;
                return zq.indexOf(Constants.BATCHDATE) != -1
                        && t1.f8 != null
                        && "A".equals(t1.f8.trim())
                        && "P".equals(t1.f7.trim())
                        && ("Purchase Invoices".equals(t1.f2.trim()) || "Sales Invoices".equals(t1.f2.trim()))
                        && ("Payables".equals(t1.f3.trim()) || "Receivables".equals(t1.f3.trim()));
            }
        });
    }

}

