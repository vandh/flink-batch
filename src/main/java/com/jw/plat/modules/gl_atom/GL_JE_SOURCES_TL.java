package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple14;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;

public class GL_JE_SOURCES_TL {
    /**
     * 抽取的列数据：
     *0: je_source_name 0
     *1: language 1
     *2: user_je_source_name 6
     * @param env
     * @return
     */
    private final static String COLMASK = "1100001";

    public static DataSet<Tuple3<String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_SOURCES_TL, Constants.batch));
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

