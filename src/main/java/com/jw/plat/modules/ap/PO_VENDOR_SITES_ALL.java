package com.jw.plat.modules.ap;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple12;
import org.apache.flink.api.java.tuple.Tuple2;

public class PO_VENDOR_SITES_ALL {
    /**
     * 抽取的列数据：
     * VENDOR_SITE_ID       30
     * VENDOR_SITE_CODE     34
     * @param env
     * @return
     */
    private final static String COLMASK = "0000000000000000000000000000010001";

    public static DataSet<Tuple2<String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.PO_VENDOR_SITES_ALL, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class).filter(
                                new FilterFunction<Tuple2<String, String>>() {
                    @Override
                    public boolean filter(Tuple2<String, String> t) throws Exception {
                        return false;
                    }
                });
    }

    public static void main(String[] args) {
        System.out.println("0000000000000000000000000000010001".length());
    }

}

