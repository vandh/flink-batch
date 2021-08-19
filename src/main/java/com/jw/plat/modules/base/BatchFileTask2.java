package com.jw.plat.modules.base;

import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.DBUtil;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.types.Row;
import scala.collection.immutable.Stream;

public class BatchFileTask2 {
    private String biz;
    private String output;
    private ExecutionEnvironment env;


    public BatchFileTask2(String biz, String output, ExecutionEnvironment env) {
        this.biz = biz;
        this.output = output;
        this.env = env;
    }

    private void proc(DataSet<Row> dataSet) {
        Tuple3<BaseExecute, String, String> t3 = Constants.BIZMAP.get(biz);
        if (output.equalsIgnoreCase(Constants.OP_DB))
            dataSet.output(DBUtil.insertMysql3(t3.f1, DBUtil.getSqlTypes(t3.f1)));
        else if (output.equalsIgnoreCase(Constants.OP_FILE)) {
            if (dataSet == null) return;
            dataSet.map(new MapFunction<Row, String>() {
                        @Override
                        public String map(Row row) throws Exception {
                            StringBuilder sb = new StringBuilder();
                            for (int i = 0; i < row.getArity(); i++)
                                if (i == row.getArity() - 1)
                                    sb.append(row.getField(i));
                                else
                                    sb.append(row.getField(i)).append("\u0007");
                            return sb.toString();
                        }
                    })
                    .writeAsText(t3.f2+"-"+Constants.batch, FileSystem.WriteMode.OVERWRITE);
            //.writeAsCsv(t3.f2, FileSystem.WriteMode.OVERWRITE);
        }
    }

    public void run() {
        if (biz.equalsIgnoreCase(Constants.JH)) {
            for (String key : Constants.BIZMAP.keySet()) {
                if (Constants.BIZMAP.get(key).f0 == null) continue;
                //if(key.equals("AP")) continue ;
//                if(Constants.batch.equals(Constants.JH)) {
                    for(int i=1; i<=8; i++) {
                        Constants.batch = i+"";
                        biz = key;
//                        Constants.PATH = i+"" ;
                        proc(Constants.BIZMAP.get(key).f0.run(env));
                    }
//                }
            }
        } else {
            proc(Constants.BIZMAP.get(biz).f0.run(env));
        }

//        try {
//            env.execute();
//        } catch (Exception e) {
//            e.printStackTrace();
//            throw new RuntimeException(e);
//        }
    }
}
