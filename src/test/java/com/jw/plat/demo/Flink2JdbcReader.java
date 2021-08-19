package com.jw.plat.demo;

import org.apache.flink.api.java.tuple.Tuple10;
import org.apache.flink.api.java.tuple.Tuple11;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Connection;

public class Flink2JdbcReader extends
        RichSourceFunction<Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer>> {
    private static final long serialVersionUID = 3334654984018091675L;

    private Connection connect = null;
    private PreparedStatement ps = null;

    /*
     * (non-Javadoc)
     *
     * @see org.apache.flink.api.common.functions.AbstractRichFunction#open(org.
     * apache.flink.configuration.Configuration) to use open database connect
     */
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        Class.forName("com.mysql.jdbc.Driver");
        connect = (Connection) DriverManager.getConnection("jdbc:mysql://192.168.10.221:3307", "root", "123456");
        ps = (PreparedStatement) connect
                .prepareStatement("select c1,c2,c3,c4,c5,c6,c7,c8,c9,c10,c11 from test2");
    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.flink.streaming.api.functions.source.SourceFunction#run(org.
     * apache.flink.streaming.api.functions.source.SourceFunction.SourceContext)
     * to use excuted sql and return result
     */
    @Override
    public void run(
            SourceContext<Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer>> collect)
            throws Exception {
        ResultSet resultSet = ps.executeQuery();
        while (resultSet.next()) {
            Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer> tuple =
                    new Tuple11<Integer, Float, Integer, Integer, Float, Integer, Integer, Integer, Integer, Integer, Integer>();
            tuple.setFields(resultSet.getInt(1), resultSet.getFloat(2), resultSet.getInt(3),
                    resultSet.getInt(4), resultSet.getFloat(5), resultSet.getInt(6), resultSet.getInt(7),
                    resultSet.getInt(8), resultSet.getInt(9), resultSet.getInt(10), resultSet.getInt(11));
            collect.collect(tuple);
        }

    }

    /*
     * (non-Javadoc)
     *
     * @see
     * org.apache.flink.streaming.api.functions.source.SourceFunction#cancel()
     * colse database connect
     */
    @Override
    public void cancel() {
        try {
            super.close();
            if (connect != null) {
                connect.close();
            }
            if (ps != null) {
                ps.close();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

}
