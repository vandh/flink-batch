package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;

public class FND_USER {
    /**
     * 抽取的列数据：
     * 0：user_id
     * 1：user_name
     * 2：last_update_date
     * @param env
     * @return
     */
    private final static String COLMASK = "111";

    public static DataSet<Tuple3<String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.FND_USER, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class)
                .filter(new FilterFunction<Tuple3<String, String, String>>() {
                    @Override
                    public boolean filter(Tuple3<String, String, String> stringStringStringTuple3) throws Exception {
                        return true;
                    }
                });
    }

}

