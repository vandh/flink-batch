package com.jw.plat.demo;

import com.jw.plat.common.filter.FilterCols;
import com.jw.plat.common.join.JoinTuple22;
import com.jw.plat.common.map.MapTuple2;
import com.jw.plat.common.select.SelectTuple2;
import com.jw.plat.common.select.SelectTuple3;
import com.jw.plat.common.util.Constants;
import com.jw.plat.common.util.MysqlUtil;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.io.OutputFormat;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.types.Row;

import java.sql.Types;

public class BatchJoin2Mysql {
    public static OutputFormat insertMysql(){
        OutputFormat insertMysql = MysqlUtil.buildMysqlUtil()
                .setQuery(Utils.T_SQL2)
                .setSqlTypes(new int[] {Types.VARCHAR, Types.INTEGER,Types.INTEGER,Types.INTEGER,Types.INTEGER})
                .finish();
        return insertMysql;
    }

    public static void main(String[] args) throws Exception{
        String propertiesPath = Batch2DBJoin.class.getResource("/").getPath()+"conf.properties"; //args[0];
        ParameterTool pm = ParameterTool.fromPropertiesFile(propertiesPath);
        Constants.DRIVER = pm.get("driver");
        Constants.URL = pm.get("url");
        Constants.USER = pm.get("username");
        Constants.PASS = pm.get("password");
        Constants.BATCHSIZE = Integer.valueOf(pm.get("batchSize"));
        String sql1 = pm.get("sql1");
        int[] sql1Types = new int[] {Types.INTEGER, Types.VARCHAR,Types.INTEGER,Types.INTEGER,Types.INTEGER};

        String kkPath1 = pm.get("kkPath1");
        String kkPath3 = pm.get("kkPath3");
        String kkPath2 = pm.get("kkPath2");

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(8);
        //DataStreamSource<String> fs2 = env.readTextFile(kkPath3);
        DataSet<Tuple2<String,Integer>> input1 = env.readTextFile(kkPath1).filter(new FilterCols()).map(new MapTuple2());
        DataSet<Tuple2<String,Integer>> input3 = env.readTextFile(kkPath3).filter(new FilterCols()).map(new MapTuple2());
        DataSet<Tuple2<String,Integer>> input2 = env.readTextFile(kkPath2).filter(new FilterCols()).map(new MapTuple2());

        //input1.print();
        //System.out.println("^^^^^^^^^^^^^^^^^^^^^");
        //input2.print();

        //https://www.cnblogs.com/qiu-hua/p/13796777.html
        DataSet<Tuple3<String,Integer, Integer>> input13 = input1.joinWithHuge(input3)
                .where(new SelectTuple2())
                .equalTo(new SelectTuple2())
                .with(new JoinTuple22());
        //input13.print();

        DataSet<Row> input132 = input13.joinWithHuge(input2)
                .where(new SelectTuple3())
                .equalTo(new SelectTuple2())
                .with(new JoinFunction<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>, Row>() {
                    @Override
                    public Row join(Tuple3<String, Integer, Integer> stringIntegerIntegerTuple3, Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                        return null;
                    }
                });

        input132.output(insertMysql());
        input132.print();
        //env.execute("simon execute ...");
    }
}
