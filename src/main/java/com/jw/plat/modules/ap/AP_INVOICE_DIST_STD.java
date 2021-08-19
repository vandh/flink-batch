package com.jw.plat.modules.ap;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

public class AP_INVOICE_DIST_STD {
    /**
     * 抽取的列数据：
     * ACCOUNTING_DATE      1
     * INVOICE_ID   8
     * INVOICE_DISTRIBUTION_ID  161
     * @param env
     * @return
     */
    private final static String COLMASK = "10000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001";

    public static DataSet<Tuple3<String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_INVOICE_DIST_STD, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class);

//               return ds1.filter(new FilterFunction<Tuple3<String, String, String>>() {

         /*   @Override
            public boolean filter(Tuple3<String, String, String> t3) throws Exception {
                       String period_name = t3.f0.trim();
                       if(period_name!=null){
                           return period_name_1_5.indexOf(period_name) != -1;
                       }
                       return false;
                   }*/
    }

    public static void main(String[] args) {
//        String s = "";
//        for(int i = 1; i<=161; i++) {
//            s+="0";
//
//        }
//        System.out.println(s);
//        System.out.println(s.length());
    }

}

