package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.types.Row;

public class GL_JE_LINES_POST {
    /**
     GL_JE_LINES_POST    12 * 0：je_header_id                0
     * 					    13*  1：je_line_num                 1
     * 					    13 * 2：ledger_id                   0
     * 					    14 * 3：code_combination_id         1
     * 					    14 * 4：period_name                 0
     * 					    14 * 5：effective_date              0
     * 					    14 * 6：creation_date               0
     * 					    14 * 7：created_by                  0
     * 					    15 * 8：accounted_dr                1
     * 					    16 * 9：accounted_cr                1
     * 					    17 * 10：description                1
     * 					    17 * 11：gl_sl_link_id              0
     * 					    17 * 12：gl_sl_link_table           0
     * 					    18 * 13：reference_9                1
     */
    private final static String COLMASK = "110011110110001110000000000000000000000000000000000000000001101";
//    private final static String period_name_1_5 = "2021-012021-022021-032021-042021-05";
    public static DataSet<Tuple14<String,String,String,String,String,String,String,String,String,String,String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_LINES_POST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        DataSource<Tuple14<String,String,String,String,String,String,String,String,String,String,String,String,String,String>> input1 =
                csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class);
        return input1 ;
    }

}

