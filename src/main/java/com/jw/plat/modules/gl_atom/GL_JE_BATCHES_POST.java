package com.jw.plat.modules.gl_atom;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple5;

public class GL_JE_BATCHES_POST {
    /**
     * 抽取的列数据：
     * 0:je_batch_id  1
     * 1:name  5
     * 2:creation_date  13
     * 3:created_by  14
     * 4:default_period_name  17
     * @param env
     * @return
     */
    private final static String COLMASK = "10001000000011001";

    public static DataSet<Tuple5<String,String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_BATCHES_POST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        // 数组的长度。
        DataSet<Tuple5<String,String,String,String,String>> input1 =
                csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        //字段类型
                        .types(String.class,String.class,String.class,String.class,String.class);

        return input1;
    }

}

