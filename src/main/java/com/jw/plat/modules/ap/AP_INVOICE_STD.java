package com.jw.plat.modules.ap;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple11;

public class AP_INVOICE_STD {
    /**
     * 抽取的列数据：1456 10 11 14 15 17 18 28 51 77 78 90
     * invoice_id   1
     * vendor_id    4
     * invoice_num  5
     * vendor_site_id 11
     * source       15
     * description  17
     * batch_id     18
     * created_by   28
     * attribute4   51
     * org_id       90
     * gl_date      121
     * @param env
     * @return
     */
    private final static String COLMASK = "1001100000100010110000000001000000000000000000000010000000000000000000000000000000000000010000000000000000000000000000001";

    public static DataSource<Tuple11<String,String,String,String,String,String,String,String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_INVOICE_STD, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class);
    }

    public static void main(String[] args) {
        System.out.println(COLMASK.length());
        for (int i=0; i<COLMASK.length(); i++) {

            if(COLMASK.charAt(i)=='1') System.out.print((i+1)+"\t");
        }
    }
}


