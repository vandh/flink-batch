package com.jw.plat.modules.ra;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple4;

public class RA_CUST_TRX_LINE_GL_DIST {
    /**
     (57,46):          Rctlgda.Cust_Trx_Line_Gl_Dist_Id             1
     (41,11):          Rctla.Customer_Trx_Line_Id(+)                2
     (5,11):           Rctlgda.gl_posted_date 会计期间,              13
     (39,34):          Rctlgda.Customer_Trx_Id                      37
     */
    private final static String COLMASK = "1100000000001000000000000000000000001";

    public static DataSet<Tuple4< String, String,String,String >> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.RA_CUST_TRX_LINE_GL_DIST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                .lineDelimiter(Constants.LF)
                .fieldDelimiter(Constants.DEL)
                .ignoreInvalidLines()
                .types(String.class,String.class,String.class,String.class);
    }
}