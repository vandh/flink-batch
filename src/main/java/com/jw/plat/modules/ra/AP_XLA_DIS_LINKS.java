package com.jw.plat.modules.ra;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple5;

public class AP_XLA_DIS_LINKS {
    /**
     * (53,32):       Xdl.Application_Id                            1
     * (54,30):       Xdl.Ae_Header_Id                              3
     * (55,29):       Xdl.Ae_Line_Num                               4
     * (56,11):       Xdl.Source_Distribution_Type                  5
     * (57,11):       Xdl.Source_Distribution_Id_Num_1             11
     */
    private final static String COLMASK = "10111000001";
    static int inx;

    public static DataSet<Tuple5<String, String, String, String, String>> proc(ExecutionEnvironment env) {

        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_DIS_LINKS, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class, String.class, String.class)
                .filter(
                        new FilterFunction<Tuple5<String, String, String, String, String>>() {
                            @Override
                            public boolean filter(Tuple5<String, String, String, String, String> t1) throws Exception {
                             //todo 正式时候将下面此段放开上面的需要进行删除
                                return "RA_CUST_TRX_LINE_GL_DIST_ALL".equals(t1.f3.trim());
                            }
                        }
                );
    }


    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName("D:\\bus\\ys", "D_20210715_AP_XLA_DIS_LINKS_08", "8"));
        csvReader.setCharset("utf8");
        csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class, String.class, String.class)
                .filter(new FilterFunction<Tuple5<String, String, String, String, String>>() {
                            @Override
                            public boolean filter(Tuple5<String, String, String, String, String> t1) throws Exception {
                                if( ++inx > 20000) return false;
//                                return "RA_CUST_TRX_LINE_GL_DIST_ALL".equals(t1.f3.trim());
                                return true;
                            }
                        }).print();

    }
}