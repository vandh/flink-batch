package com.jw.plat.demo;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * DROP TABLE test2;
 * CREATE TABLE test2(c0 INT NOT NULL AUTO_INCREMENT PRIMARY KEY, c1 INT,c2 FLOAT,c3 INT, c4 INT, c5 FLOAT, c6 INT,c7 INT,c8 INT, c9 INT,c10 INT,c11 INT);
 */
public class Flink2JdbcWriter extends
        RichSinkFunction<Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer>> {
    private static final long serialVersionUID = -8930276689109741501L;

    private Connection connect = null;
    private PreparedStatement ps = null;
    private final int BATCH_SEZE = 3000;
    private int count;
    /*
     * (non-Javadoc)
     *
     * @see org.apache.flink.api.common.functions.AbstractRichFunction#open(org.
     * apache.flink.configuration.Configuration) get database connect
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.cj.jdbc.Driver");
        connect = (Connection) DriverManager.getConnection("jdbc:mysql://192.168.10.221:3307/db_2", "root", "123456");
        connect.setAutoCommit(false);
        ps = (PreparedStatement) connect.prepareStatement("insert into test2(c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11) values (?,?,?,?,?,?,?,?,?,?,?)");
        System.out.println("...open conn, ps");
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.flink.streaming.api.functions.sink.SinkFunction#invoke(java.
     * lang.Object,
     * org.apache.flink.streaming.api.functions.sink.SinkFunction.Context) read
     * data from flink DataSet to database
     */
    @Override
    public void invoke(Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer> value,
                       Context context) throws Exception {
        count++;
        ps.setInt(1, value.f0);
        ps.setFloat(2, value.f1);
        ps.setInt(3, value.f2);
        ps.setInt(4, value.f3);
        ps.setFloat(5, value.f4);
        ps.setInt(6, value.f5);
        ps.setInt(7, value.f6);
        ps.setInt(8, value.f7);
        ps.setInt(9, value.f8);
        ps.setInt(10, value.f9);
        ps.setInt(11, value.f10);

        try {
//            log.info("...cur rows count "+count+"! ");
//            ps.executeUpdate();
//            connect.commit();
//            if (count % BATCH_SEZE == 0) {
//                ps.executeBatch();
//                connect.commit();
//                log.info("...commit "+BATCH_SEZE+"! ");
//            } else
                ps.addBatch();
        } catch (Exception e) {
            e.printStackTrace();
            connect.rollback();
            System.out.println("...rollback "+BATCH_SEZE+"! ");
            throw e;
        }
    }

    /*
     * (non-Javadoc)
     *
     * @see org.apache.flink.api.common.functions.AbstractRichFunction#close()
     * close database connect
     */
    @Override
    public void close() throws Exception {
        try {
            ps.executeBatch();
            super.close();
            if (ps != null) {
                ps.close();
            }
            if (connect != null) {
                connect.commit();
                connect.close();
                System.out.println("...last commit & close conn! = "+count);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
