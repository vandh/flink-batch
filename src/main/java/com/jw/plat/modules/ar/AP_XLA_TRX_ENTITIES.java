package com.jw.plat.modules.ar;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple5;

public class AP_XLA_TRX_ENTITIES {
    /**
     * (49,11):       And Xte.Entity_Id = Xe.Entity_Id                                    1
     * (48,11):       And Xte.Application_Id = Xe.Application_Id                          2
     * (46,11):       And Xte.Entity_Code = 'RECEIPTS'                                    4
     * (47,15):       And Nvl(Xte.Source_Id_Int_1, -99) = Acra.Cash_Receipt_Id            10
     * (45,11):       And Xte.Ledger_Id = Acra.Set_Of_Books_Id                            25
     */
    private final static String COLMASK = "1101000001000000000000001";

    public static DataSet<Tuple5<String, String, String, String, String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.AP_XLA_TRX_ENTITIES, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class, String.class, String.class, String.class, String.class)
                .filter(new FilterFunction<Tuple5<String, String, String, String, String>>() {
                    @Override
                    public boolean filter(Tuple5<String, String, String, String, String> t1) throws Exception {
                        if (t1.f2 != null && "RECEIPTS".equals(t1.f2.trim())){
                            t1.f3 = t1.f3 == null || "".equals(t1.f3.trim()) ? "-99" : t1.f3.trim();
                            return true;
                        }
                        return false;
                    }
                });
    }
}

