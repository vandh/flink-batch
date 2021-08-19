package com.jw.plat.demo;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;

import java.io.FileInputStream;
import java.io.FileOutputStream;

public class Other {
    public static void main(String[] args) throws Exception{
//        String glFile = "D:/flink/GL_JE_LINES_POST";
//        String glFile2 = "D:/flink/GL_JE_LINES_POST2";
//        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        env.setParallelism(1);
//        env.readTextFile("D:/flink/GL_JE_LINES_POST","GBK").map(new MapFunction<String, Object>() {
//            long count;
//            @Override
//            public Object map(String s) throws Exception {
//                count++;
////                String[] ss = s.split(",");
////                for(int i=0; i<ss.length; i++)
////                    System.out.print(ss[i]+"\t");
//                s = s.replaceAll("\u0007",",");
//                s = s.replaceAll("\u0006","\n");
//                System.out.println(s); //
//                if(count<=10) System.exit(0);
//                return s;
//            }
//        });
//
//        FileInputStream fis = new FileInputStream(glFile);
//        FileOutputStream fos = new FileOutputStream(glFile2);
//        byte[] b = new byte[1024*1024];
//        if(fis.read(b)>0)
//            fos.write(b);

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

//        env.setParallelism(8);

        String s = "60483214,2,2020-01-01 16:50:43,5975,2021,40522308,2019-12,2019-12-31 00:00:00,P,2020-01-01 16:46:25,18542,,,140586.28,,140586.28,已创建日记帐导入,,,,,,903014028,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,921147515,911474990,2345884192,XLAJEL,2,,140586.28,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,,2020-01-03 01:39:07";
        System.out.println(s.split(",").length);
        System.out.println("1100111100000011100000000000000000000000000000000000000000011".length());
    }
}
