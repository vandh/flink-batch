package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple4;

public class QGL_SEQUENCE_VALUE {
    /**
     * 抽取的列数据：
     * je_header_id
     * sequence_value
     * last_update_date
     * ods_creation_date
     * @param env
     * @return
     */
    private final static String COLMASK = "1111";

    public static DataSet<Tuple4<String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.QGL_SEQUENCE_VALUE, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .types(String.class,String.class,String.class,String.class);
    }

    public static void main(String[] args) throws Exception {
        QGL_SEQUENCE_VALUE a = new QGL_SEQUENCE_VALUE();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        Constants.PATH = "I:/flink/dw01";
        Constants.batch = "1";
        env.setParallelism(1);
                Constants.GLCHARSET="GBK";
        Constants.BATCHDATE=null;


        a.proc(env).print();

        env.execute();
    }

}

