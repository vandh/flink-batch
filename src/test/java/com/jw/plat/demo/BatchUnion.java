package com.jw.plat.demo;

import com.jw.plat.common.max.MaxTuple3;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple3;

public class BatchUnion {
    public synchronized static void proc(ExecutionEnvironment env, String outpath, long timeout) throws Exception{
//        String propertiesPath = Batch2DBJoin.class.getResource("/").getPath()+"conf.properties"; //args[0];
//        ParameterTool pm = ParameterTool.fromPropertiesFile(propertiesPath);
//        Constants.DRIVER = pm.get("driver");
//        Constants.URL = pm.get("url");
//        Constants.USER = pm.get("username");
//        Constants.PASS = pm.get("password");
//        Constants.BATCHSIZE = Integer.valueOf(pm.get("batchSize"));
//        String sql1 = pm.get("sql1");
//        int[] sql1Types = new int[] {Types.INTEGER, Types.VARCHAR,Types.INTEGER,Types.INTEGER,Types.INTEGER};

        String kkPath1 = "d:/flink2/kk1";
        String kkPath3 = "d:/flink2/kk3";
        String kkPath2 = "d:/flink2/kk2";


        //设置递归参数
        //Configuration conf = new Configuration();
        //conf.setBoolean("recursive.file.enumeration",true);
        //DataSource<String> ds = env.readTextFile("D:\\testData").withParameters(conf);
//        DataSource<Tuple3<Integer, Integer, String>> csvDS = env.readCsvFile("D:\\Downloads\\000代码+PPT\\数据源\\user.csv")
//                .includeFields("11100")     //对于csv文件中的字段是否读取1代表读取,0代表不读取
//                .ignoreFirstLine()          //是否忽视第一行
//                .ignoreInvalidLines()       //对于不合法的行是否忽视
//                .ignoreComments("##")       //忽视带有##号的行
//                .lineDelimiter("\n")        //行分隔符
//                .fieldDelimiter(",")        //列分隔符
//                .types(Integer.class,Integer.class,String.class);


        //DataStreamSource<String> fs2 = env.readTextFile(kkPath3);
        DataSource<Tuple3<Integer,Integer,Integer>> input1 = env.readCsvFile(kkPath1).includeFields("1011").lineDelimiter("\n").fieldDelimiter(",").types(Integer.class,Integer.class,Integer.class);
        DataSource<Tuple3<Integer,Integer,Integer>> input2 = env.readCsvFile(kkPath2).includeFields("1011").lineDelimiter("\n").fieldDelimiter(",").types(Integer.class,Integer.class,Integer.class);
        DataSource<Tuple3<Integer,Integer,Integer>> input3 = env.readCsvFile(kkPath3).includeFields("1011").lineDelimiter("\n").fieldDelimiter(",").types(Integer.class,Integer.class,Integer.class);

        //input1.union(input2).print();

        long c = 0;
        input1.union(input2).union(input3).groupBy(0).reduce(new MaxTuple3()).writeAsCsv(outpath);
//        log.error(String.valueOf(c));

        System.out.println(outpath+"\t"+timeout);
        Thread.sleep(timeout);
    }
}
