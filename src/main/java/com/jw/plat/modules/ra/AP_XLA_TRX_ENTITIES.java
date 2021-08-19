package com.jw.plat.modules.ra;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple5;

public class AP_XLA_TRX_ENTITIES {
    /**
     (48,11):        Xte.Entity_Id                  1
     (47,11):        Xte.Application_Id             2
     (45,11):        Xte.Entity_Code                4
     (46,15):        Xte.Source_Id_Int_1            10
     (44,11):        Xte.Ledger_Id                  25
     */
    private final static String COLMASK = "1101000001000000000000001";

    public static DataSet<Tuple5< String,String,String,String,String >> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_TRX_ENTITIES, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class,String.class,String.class)
            .filter(new FilterFunction<Tuple5< String,String,String,String,String>>() {
                @Override
                public boolean filter(Tuple5< String,String,String,String,String> t1) throws Exception {
                   if(t1.f2!=null && "TRANSACTIONS".equals(t1.f2.trim())) {
                        t1.f3 = t1.f3==null || "".equals(t1.f3.trim()) ? "-99" : t1.f3.trim();
                        return true;
                    }
                    return false;
                }
            });
        }

        static  int i = 0;

    public static void main(String[] args) throws Exception {

            ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
            CsvReader csvReader = env.readCsvFile(FileUtil.getFileName("D:\\bus\\ys", "D_20210715_AP_XLA_TRX_ENTITIES_08", "8"));
            csvReader.setCharset("utf8");

           csvReader.includeFields(COLMASK)
                    .lineDelimiter(Constants.LF)
                    .fieldDelimiter(Constants.DEL)
                    .ignoreInvalidLines()
                    .types(String.class,String.class,String.class,String.class,String.class)

                    .filter(new FilterFunction<Tuple5< String,String,String,String,String>>() {
                        @Override
                        public boolean filter(Tuple5< String,String,String,String,String> t1) throws Exception {
//                            i++;
//                           if(t1.f2!=null && "TRANSACTIONS".equals(t1.f2.trim())) {
//                                t1.f3 = t1.f3==null || "".equals(t1.f3.trim()) ? "-99" : t1.f3.trim();
//                                return true;
//                            }
//                            return false;
                            return true;
                        }

                    }).print();
    }
}