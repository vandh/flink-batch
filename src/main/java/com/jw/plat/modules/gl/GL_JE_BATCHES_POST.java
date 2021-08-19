package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FilterOperator;
import org.apache.flink.api.java.tuple.Tuple7;

import javax.imageio.stream.ImageInputStream;

import static com.jw.plat.common.util.Constants.BATCHDATE;

public class GL_JE_BATCHES_POST {
    /**
     * 抽取的列数据：
     * je_batch_id  1
     * name  5
     * creation_date  13
     * created_by  14
     * default_period_name  17
     * org_id   46
     * group_id  51
     * @param env
     * @return
     */
    private final static String COLMASK = "100010000000110010000000000000000000000000000100001";

    public static DataSet<Tuple7<String,String,String,String,String,String,String>> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.GL_JE_BATCHES_POST, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        // 数组的长度。
        DataSet<Tuple7<String,String,String,String,String,String,String>> input1 =
                csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        //字段类型
                        .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class);

        // where 单表条件过滤数据方式在这个过滤器中。
        return input1.filter(new FilterFunction<Tuple7<String,String,String,String,String,String,String>>() {
            @Override
            public boolean filter(Tuple7<String, String, String, String, String, String, String> t1) throws Exception {
                /*return t1.f0.trim().indexOf("8866616")!=-1;*/
                if(Constants.BATCHDATE==null || Constants.BATCHDATE.trim().equals("")) return true;
                String zq = t1.f4.trim();
//                return true;
                return zq.indexOf(Constants.BATCHDATE) != -1;
            }
        });
    }

    public static void main(String[] args) {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName("C:\\Users\\vandh\\Desktop\\dw", Constants.GL_JE_BATCHES_POST, "1"));
        csvReader.setCharset("GBK");
        try {
            csvReader.includeFields(COLMASK)
                            .lineDelimiter(Constants.LF)
                            .fieldDelimiter(Constants.DEL)
                            .ignoreInvalidLines()
                            //字段类型
                            .types(String.class,String.class,String.class,String.class,String.class,String.class,String.class)
            .print();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}

