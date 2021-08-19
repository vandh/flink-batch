package com.jw.plat.dealDateController;

import com.jw.plat.common.util.Constants;
import com.jw.plat.modules.ap.*;
import com.jw.plat.modules.base.BaseExecute;
import com.jw.plat.modules.base.BatchFileTask;
import com.jw.plat.modules.gl_atom.GLExecute;
import com.jw.plat.modules.initData.LoadData2Mysql;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import sun.nio.cs.ext.GBK;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

/**
 * 参数： 文件位置   批次(1~8)  字符集  账期             业务类别  并行任务数  输出介质  输出文件位置
 * 账期如果有多个，以,分隔，无账期则 - 代替
 * 业务类别为AP/GL/APGL，如果跑所有的，以 - 代替
 * 如果要定时跑任务
 * 如：   d:/flink    2       GBK  2021-04            APPRE|APSTD|GL|APGL.   2    FILE|DB  /data/output
 * 如： java -jar dw.jar d:/flink  2  GBK  - APGL 2  FILE d:/
 *
 */
public class Main {
    private static void init(String[] args) throws Exception{
        if(args==null || args.length<8) {
            System.err.println("please assign params : path batch charset date BIZ parallet outType outfilePath!");
            System.exit(1);
        }

        String propertiesPath = args[0]+File.separator+"conf.properties";
        System.out.println("conf.properties: "+propertiesPath);

        ParameterTool pm = ParameterTool.fromPropertiesFile(propertiesPath);
        Constants.DRIVER = pm.get("driver");
        Constants.URL = pm.get("url");
        Constants.USER = pm.get("username");
        Constants.PASS = pm.get("password");
        Constants.BATCHSIZE = Integer.valueOf(pm.get("batchSize"));
        //Constants.PATH = pm.get("glpath");
        Constants.PATH = args[0];
        Constants.batch = args[1];
        Constants.GLSQL = pm.get("glsql");
        Constants.APSQL = pm.get("apsql");
        Constants.GLAPSQL = pm.get("apglsql");
        //Constants.GLCHARSET = pm.get("glcharset");
        Constants.GLCHARSET = args[2].toUpperCase();
        Constants.BATCHDATE = args[3].trim();
        if(Constants.JH.equals(Constants.BATCHDATE)) Constants.BATCHDATE="";
        if(!(new File(Constants.PATH).exists())) {
            System.err.println(Constants.PATH+" not exists!");
            System.exit(1);
        }
    }

    public static void main(String[] args) throws Exception {
        //  I:/flink/dw01  1 GBK 2021-02 GL 1  FILE I:/flink/result
        if(args[0].equalsIgnoreCase("DB")) {
            new LoadData2Mysql(args).load();
            return;
        }
        init(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        int parell = Integer.valueOf(args[5].trim());//执行任务数量
        String biz = args[4].trim();  // 读的文件类型
      /*  String[] bizArr = {Constants.GLPRE,Constants.GLSTD,Constants.APPRE,Constants.APSTD,Constants.GLAP};
        if(!Arrays.asList(bizArr).contains(biz)) {
            System.err.println("designed the ap|gl parameter wrong!");
            System.exit(1);
        }*/
        String output = args[6].trim();
        env.setParallelism(parell <= 0 ? 1 : parell > 8 ? 8 : parell); //设置执行任务数量
        String now = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Constants.OUTPATH = args[7];

        Constants.BIZMAP = new HashMap<String, Tuple3<BaseExecute, String,String>>();
        // 处理的文件类型
        Constants.BIZMAP.put(Constants.GL, Tuple3.of(new GLExecute(), Constants.GLSQL,
                args[7]+File.separator+Constants.GL+"-"+Constants.batch));
        Constants.BIZMAP.put(Constants.APPRE, Tuple3.of(new APExecute_PRE(), Constants.APSQL,
                args[7]+File.separator+Constants.APPRE+"-"+Constants.batch));
        Constants.BIZMAP.put(Constants.APSTD, Tuple3.of(new APExecute_STD(), Constants.APSQL,
                args[7]+File.separator+Constants.APSTD+"-"+Constants.batch));
        Constants.BIZMAP.put(Constants.GLAP, Tuple3.of(new APGLExecute(), Constants.GLAPSQL,
                args[7]+File.separator+Constants.GLAP+"-"+Constants.batch));



        Constants.BIZMAP.put(Constants.APSTD2, Tuple3.of(new APExecute_STD2(), Constants.APSQL,
                args[7]+File.separator+Constants.APSTD2+"-"+Constants.batch));
        Constants.BIZMAP.put(Constants.APSTD3, Tuple3.of(new APExecute_STD3(), Constants.APSQL,
                args[7]+File.separator+Constants.APSTD3+"-"+Constants.batch));
        Constants.BIZMAP.put(Constants.APSTD4, Tuple3.of(new APExecute_STD4(), Constants.APSQL,
                args[7]+File.separator+Constants.APSTD4+"-"+Constants.batch));



//        ScheduledExecutorService scheduledExecutorService = Executors.newScheduledThreadPool(1);
//        ScheduledFuture future = scheduledExecutorService.schedule(
//                new BatchFileTask(biz, output, env),
//                0, TimeUnit.SECONDS);
//
//        future.get();
        new BatchFileTask(biz, output, env).run();
        env.execute();

        /**
         * 1.跑增量文件
         * 2.增量文件入库   flink run -c com.jw.plat.Main2 /data/flink/data-flink-1.0-SNAPSHOT.jar DB 10.242.29.19 10198 rtp_dw_db root j67XRxOY2440 /data/flinkadd/202106152 AP_STD_VOUCHER_INFO_1
         * 3.入库的24增量张表跑出2张rtp表
         * 4.24张增量表入全量表
         * 5.rtp表入mycat库
         */

    }


}
