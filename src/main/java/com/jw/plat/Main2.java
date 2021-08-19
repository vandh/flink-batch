package com.jw.plat;

import com.jw.plat.common.util.Constants;
import com.jw.plat.modules.ap.APExecute_PRE;
import com.jw.plat.modules.ap.APExecute_STD;
import com.jw.plat.modules.base.BaseExecute;
import com.jw.plat.modules.base.BatchFileTask3;
import com.jw.plat.modules.gl_atom.GLExecute;
import com.jw.plat.modules.initData.LoadData2Mysql3;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

/**
 * 数据文件：D:\project\2021\JW\flink\080201
 * 表结构与sql：dw数据库表.rar，本项目下
 * 参数： 1.需要读的文件所在目录  2.批次  3.字符 4.账期 5.读的文件类别 APPRE|APSTD|GL|APGL 6.任务数  7.到文件或数据库 file/db 8.输出目录
 * 账期如果有多个，以,分隔，无账期则 - 代替
 * 业务类别为AP/GL/APGL，如果跑所有的，以 - 代替
 * 如果要定时跑任务
 * 如：   d:/flink    2       GBK  2021-04            APPRE|APSTD|GL|APGL.   2    FILE|DB  /data/output
 * 如： java -jar dw.jar d:/flink  2  GBK  - APPRE 2  FILE d:/flink
 * flink run -c com.jw.plat.Main2 /data/dbtool/data-flink-1.0-SNAPSHOT.jar DB 10.238.25.109 10072 rtp_dw_db root evBKIA27vUap /data/testra/RA-50 ra
 */
public class Main2 {
    private static final Logger logger= LoggerFactory.getLogger(Main2.class);
    private static void init(String[] args) throws Exception {
        Constants.PROPERTIESPATH  ="/data/dbtool/conf.properties";

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

    public static void pproc(String[] args) throws Exception {
//        if (args[1].equalsIgnoreCase("TRANS")) {
//            //add 到rtp  todo   //add的rtp表到全量的rtp表去重查询
//            TransAddDataToRtpInfo.trans(args);
//            //add 到全量
//            TransDwAddToAll.trans(args);
//            //rtp 到 mycat 的业务表
//            //TransDwToService.trans(args);//E:\\项目资料\\04.IBM关联交易\\conifg  TRANS
//            return;
//        }
        init(args);
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        int parell = Integer.valueOf(args[5].trim());
        String biz = args[4].trim();
        String output = args[6].trim();
        env.setParallelism(parell <= 0 ? 1 : parell > 8 ? 8 : parell);
        String now = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        Constants.OUTPATH = args[7];

        Constants.BIZMAP = new HashMap<String, Tuple3<BaseExecute, String, String>>();

        Constants.BIZMAP.put(Constants.GL, Tuple3.of(new GLExecute(), Constants.GLSQL,
                args[7] + File.separator + Constants.GL));
        Constants.BIZMAP.put(Constants.APPRE, Tuple3.of(new APExecute_PRE(), Constants.APPRESQL,
                args[7] + File.separator + Constants.APPRE));
        Constants.BIZMAP.put(Constants.APSTD, Tuple3.of(new APExecute_STD(), Constants.APSTDSQL,
                args[7] + File.separator + Constants.APSTD));
        new BatchFileTask3(biz, output, env).run();
        env.execute();

        //从上面抽出的文件中进行数据处理
        // DB 10.242.29.19 10121 rtp_dw_db root GA81Da81bO34 /data/flink/APSTD-2 AP_STD_VOUCHER_INFO_2
        // 生产文件后导入数据库
//        new LoadData2Mysql4(args).load();

    }

    public static void main(String[] args) throws Exception {
//        args = new String[]{"D:\\project\\2021\\JW\\flink\\080201","1","GBK","-","APPRE","1","FILE","D:\\project\\2021\\JW\\flink\\rs"};
        if (args[0].equalsIgnoreCase("DB")) {
            new LoadData2Mysql3(args).load();
            return;
        }
        if (args[4].equals("+") && args[1].equals("+")) {
            String[] biz = {"GL", "APRPE", "APSTD"};
            for (String z : biz) {
                for (int i = 1; i <= 8; i++) {
                    args[4] = z;
                    args[1] = i + "";
                    pproc(args);
                }
            }
        } else {
            pproc(args);
        }
    }


}
