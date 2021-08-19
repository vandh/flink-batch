package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.types.Row;

public class GL_JE_LINES_POST {
    /**
     * 抽取的列数据：
     * je_header_id
     * je_line_num
     * ledger_id
     * code_combination_id
     * period_name
     * effective_date
     * creation_date
     * created_by
     * accounted_dr
     * accounted_cr
     * description
     * gl_sl_link_id
     * gl_sl_link_table
     * reference_9
     * @param env
     * @return
     */
    private final static String COLMASK = "110011110110001110000000000000000000000000000000000000000001101";

    public static DataSet<Tuple14<String,String,String,String,String,String,String,String,String,String,String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_LINES_POST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        DataSource<Tuple14<String,String,String,String,String,String,String,String,String,String,String,String,String,String>> input1 =
                csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class);
        return input1.filter(new FilterFunction<Tuple14<String,String,String,String, String, String, String, String, String, String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple14<String,String, String,String,String, String, String, String, String, String, String, String, String, String> t1) throws Exception {
                if(Constants.BATCHDATE==null || Constants.BATCHDATE.trim().equals("")) return true;
                String zq = t1.f4.trim();
//                return true;
                return zq.indexOf(Constants.BATCHDATE) != -1;}
        });
//                .groupBy(3)
//                .reduce(new ReduceFunction<Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String>>() {
//                    @Override
//                    public Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String> reduce(Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String> t1,
//                                                                                                                                                          Tuple14<String, String, String, String, String, String, String, String, String, String, String, String, String, String> t2) throws Exception {
//                        return t1;
//                    }
//                });
    }

    public static MapOperator<Tuple14<String,String, String,String,String, String, String, String, String, String, String, String, String, String>, Row> proc2(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_LINES_POST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        DataSource<Tuple14<String,String,String,String,String,String,String,String,String,String,String,String,String,String>> input1 =
                csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class,String.class);

        FilterOperator<Tuple14<String,String,String,String,String,String,String,String,String,String,String,String,String,String>> row =
                input1.filter(new FilterFunction<Tuple14<String,String,String,String, String, String, String, String, String, String, String, String, String, String>>() {
            @Override
            public boolean filter(Tuple14<String,String,String,String, String, String, String, String, String, String, String, String, String, String> t1) throws Exception {
                if(Constants.BATCHDATE==null || Constants.BATCHDATE.trim().equals("")) return true;
                String zq = t1.f4.trim();
//                return true;
                return Constants.BATCHDATE.indexOf(zq) != -1;
            }
        });
        return row.map(new MapFunction<Tuple14<String,String, String,String,String, String, String, String, String, String, String, String, String, String>, Row>() {
            @Override
            public Row map(Tuple14<String, String,String,String,String, String, String, String, String, String, String, String, String, String> t1) throws Exception {
                Row row = new Row(13);
                row.setField(0, t1.f0);
                row.setField(1, t1.f1);
                row.setField(2, t1.f2);
                row.setField(3, t1.f3);
                row.setField(4, t1.f4);
                row.setField(5, t1.f5);
                row.setField(6, t1.f6);
                row.setField(7, t1.f7);
                row.setField(8, t1.f8);
                row.setField(9, t1.f9);
                row.setField(10, t1.f10);
                row.setField(11, t1.f11);
                row.setField(12, t1.f12);
                return row;
            }
        });
    }
}

