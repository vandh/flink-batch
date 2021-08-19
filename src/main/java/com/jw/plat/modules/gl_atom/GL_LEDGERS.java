package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple2;

public class GL_LEDGERS {
    /**
     * 抽取的列数据：
     * LEDGER_ID
     * CHART_OF_ACCOUNTS_ID
     * @param env
     * @return
     */
    private final static String COLMASK = "10000000001";

    public static DataSet<Tuple2<String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.FND_USER, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class)
                .filter(new FilterFunction<Tuple2<String, String>>() {
                    @Override
                    public boolean filter(Tuple2<String, String> stringStringStringTuple3) throws Exception {
                        return true;
                    }
                });
    }

}

