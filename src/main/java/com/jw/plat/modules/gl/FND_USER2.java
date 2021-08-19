package com.jw.plat.modules.gl;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.FileUtil;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.io.CsvReader;
import org.apache.flink.api.java.tuple.Tuple3;

public class FND_USER2 {
    /**
     * 抽取的列数据：
     * user_id
     * user_name
     * last_update_date
     * @param env
     * @return
     */
    private final static String COLMASK = "111";

    public static DataSet<User> proc(ExecutionEnvironment env) {
        CsvReader csvReader = env.readCsvFile(FileUtil.getFileName(Constants.PATH, Constants.FND_USER, Constants.batch));
        csvReader.setCharset(Constants.GLCHARSET);
        return csvReader.includeFields(COLMASK)
                        .lineDelimiter(Constants.LF)
                        .fieldDelimiter(Constants.DEL)
                        .ignoreInvalidLines()
                        .pojoType(User.class, "id","name","age")
                .filter(new FilterFunction<User>() {
                    @Override
                    public boolean filter(User user) throws Exception {
                        return false;
                    }
                });
    }

    class User {
        String id;
        String name;
        String age;
    }

}

