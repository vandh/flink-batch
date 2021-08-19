package com.jw.plat.dealDateController;

import com.jw.plat.common.util.Constants;
import com.jw.plat.modules.ap.APExecute_PRE;
import com.jw.plat.modules.ap.APExecute_STD;
import com.jw.plat.modules.base.BaseExecute;
import com.jw.plat.modules.base.BatchFileTask3;
import com.jw.plat.modules.gl_atom.GLExecute;
import com.jw.plat.modules.initData.LoadData2Mysql3;
import com.jw.plat.modules.initData.TransAddDataToRtpInfo;
import com.jw.plat.modules.ra.RAExecute;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;

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
 * 批量跑数据
 * I:/flink/dw01  -  GBK - - 1  FILE I:/flink/result
 * 指定入库
 * DB 10.242.29.19 10121 rtp_dw_db root GA81Da81bO34 /data/flink/APSTD-2 AP_STD_VOUCHER_INFO_2
 * 增量的数据各批次一次性入库
 * DB 127.0.0.1 3306 base root 123 I:\\flink\\dw15add AP_STD_VOUCHER_INFO_1
 * 边跑边入库
 * I:/flink/dw01  1  GBK - - 1  DB I:/flink/result
 */

//综合 读取文件配置以及写数据的配置

/**
 * 1.需要读的文件  2.批次  3.字符 4.账期 5.读的文件小类 APPRE|APSTD|GL|APGL 6.线程数 7.file/db 8.输出目录
 *  1.DB 2.ip 3.port  4.库名  5.username  6.password 7.读取的文件按地址 8.执行到表
 * I:/flink/dw01  - GBK - - 2  FILE I:/flink/result  DB 127.0.0.1 3306 base root 123 I:\\flink\\dw15add AP_STD_VOUCHER_INFO_1
 * D:\bus\ys 8 GBK - RA 2 FILE D:\bus
 */
public class Main4 {
    private static void init(String[] args) throws Exception {
        /*if(args==null || args.length<8) {
            System.err.println("please assign params : path batch charset date BIZ parallet outType outfilePath!");
            System.exit(1);
        }*/

//        String propertiesPath = args[0] + File.separator + "conf.properties";
//        System.out.println("conf.properties: " + propertiesPath);
//        Constants.PROPERTIESPATH  ="/data/day/conf/conf.properties";
//        Constants.PROPERTIESPATH  ="E:\\jingwei\\aijw-dw-flink\\src\\main\\resources\\conf.properties";
        Constants.PROPERTIESPATH  ="/data/day/conf/conf.properties";
        ParameterTool pm = ParameterTool.fromPropertiesFile(Constants.PROPERTIESPATH);
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
        Constants.APPRESQL = pm.get("appresql");
        Constants.APSTDSQL = pm.get("apstdsql");
        Constants.GLAPSQL = pm.get("apglsql");
        //Constants.GLCHARSET = pm.get("glcharset");
        Constants.GLCHARSET = args[2].toUpperCase();
        Constants.BATCHDATE = args[3].trim();
        if (Constants.JH.equals(Constants.BATCHDATE)) Constants.BATCHDATE = "";
        if (!(new File(Constants.PATH).exists())) {
            System.err.println(Constants.PATH + " not exists!");
            System.exit(1);
        }
    }



    public static void main(String[] args) throws Exception {
        init(args);
        if (args[0].equalsIgnoreCase("DB")) {
            new LoadData2Mysql3(args).load();
            return;
        }
        if (args[1].equalsIgnoreCase("TRANS")) {
            //add 到rtp
            TransAddDataToRtpInfo.trans(args);
            //add 到全量
//            TransDwAddToAll.trans(args);
            //rtp 到 mycat 的业务表
            //TransDwToService.trans(args);//E:\\项目资料\\04.IBM关联交易\\conifg  TRANS
            return;
        }

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        int parell = Integer.valueOf(args[5].trim());
        String biz = args[4].trim();
      /*  String[] bizArr = {Constants.GLPRE,Constants.GLSTD,Constants.APPRE,Constants.APSTD,Constants.GLAP};
        if(!Arrays.asList(bizArr).contains(biz)) {
            System.err.println("designed the ap|gl parameter wrong!");
            System.exit(1);
        }*/
        String output = args[6].trim();
        env.setParallelism(parell <= 0 ? 1 : parell > 8 ? 8 : parell);
        String now = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Constants.OUTPATH = args[7];

        Constants.BIZMAP = new HashMap<String, Tuple3<BaseExecute, String, String>>();

       /* Constants.BIZMAP.put(Constants.GL, Tuple3.of(new GLExecute(), Constants.GLSQL,
                args[7] + File.separator + Constants.GL));
        Constants.BIZMAP.put(Constants.APPRE, Tuple3.of(new APExecute_PRE(), Constants.APPRESQL,
                args[7] + File.separator + Constants.APPRE));
        Constants.BIZMAP.put(Constants.APSTD, Tuple3.of(new APExecute_STD(), Constants.APSTDSQL,
                args[7] + File.separator + Constants.APSTD));
*/
        Constants.BIZMAP.put(Constants.RA, Tuple3.of(new RAExecute(), Constants.RASQL,
                args[7] + File.separator + Constants.RA));

//        future.get();
//        new BatchFileTask2(biz, output, env).run();
        new BatchFileTask3(biz, output, env).run();
        env.execute();

        //从上面抽出的文件中进行数据处理
        // DB 10.242.29.19 10121 rtp_dw_db root GA81Da81bO34 /data/flink/APSTD-2 AP_STD_VOUCHER_INFO_2
        // 生产文件后导入数据库
//        new LoadData2Mysql4(args).load();

    }




}
