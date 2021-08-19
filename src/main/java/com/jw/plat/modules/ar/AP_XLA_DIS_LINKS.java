package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple5;

public class AP_XLA_DIS_LINKS {
    /**
     * 	(54,32):       And Xal.Application_Id = Xdl.Application_Id                  1
     * 	(55,30):       And Xal.Ae_Header_Id = Xdl.Ae_Header_Id                      3
     * 	(56,29):       And Xal.Ae_Line_Num = Xdl.Ae_Line_Num                        4
     * 	(57,11):       And Xdl.Source_Distribution_Type = 'AR_DISTRIBUTIONS_ALL'    5
     * 	(58,11):       And Xdl.Source_Distribution_Id_Num_1 = Ada.Line_Id           6
     *
     *
     * --------------------------------------------------------------以下是应收数据 不采用
     (53,32):       Xdl.Application_Id                            1
     (54,30):       Xdl.Ae_Header_Id                              3
     (55,29):       Xdl.Ae_Line_Num                               4
     (56,11):       Xdl.Source_Distribution_Type                  5
     (57,11):       Xdl.Source_Distribution_Id_Num_1              6
     */
    private final static String COLMASK = "101111";

    public static DataSet<Tuple5< String,String,String,String,String >> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_DIS_LINKS, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class,String.class,String.class)
                .filter(
                new FilterFunction<Tuple5<String, String, String, String, String>>() {
                    @Override
                    public boolean filter(Tuple5<String, String, String, String, String> t1) throws Exception {
                        return "AR_DISTRIBUTIONS_ALL".equals(t1.f3);
                    }
                }
        );
    }
}