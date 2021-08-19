package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

public class GL_JE_CATEGORIES_TL {
    /**
     * 抽取的列数据：
     * 0：je_category_name 1
     * 1：language 2
     * 2：user_je_category_name 4
     * @param env
     * @return
     */
    private final static String COLMASK = "1101";

    public static DataSet<Tuple3<String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_CATEGORIES_TL, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        DataSource<Tuple3<String,String,String>> input =  csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class);
        return input.filter(new FilterFunction<Tuple3<String,String,String>>() {
            @Override
            public boolean filter(Tuple3<String, String, String> t3) throws Exception {
                if(t3.f1.trim().equals("ZHS")) return true;
                return false;
            }
        });
    }

}

