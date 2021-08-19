package com.jw.plat.common.util;

import org.apache.flink.api.java.io.jdbc.AbstractJDBCOutputFormat;
import org.apache.flink.api.java.io.jdbc.JDBCUtils;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class MysqlUtil extends AbstractJDBCOutputFormat<Row> {
    private static final long serialVersionUID = 1L;
    private static final Logger LOG = LoggerFactory.getLogger(MysqlUtil.class);
    private final String query;
    private final int batchInterval;
    private final int[] typesArray;
    private PreparedStatement upload;
    private int batchCount = 0;

    public MysqlUtil(String username, String password, String drivername, String dbURL, String query, int batchInterval, int[] typesArray) {
        super(username, password, drivername, dbURL);
        this.query = query;
        this.batchInterval = batchInterval;
        this.typesArray = typesArray;
    }

    public void open(int taskNumber, int numTasks) throws IOException {
        try {
            this.establishConnection();
            this.upload = this.connection.prepareStatement(this.query);
        } catch (SQLException var4) {
            throw new IllegalArgumentException("open() failed.", var4);
        } catch (ClassNotFoundException var5) {
            throw new IllegalArgumentException("JDBC driver class not found.", var5);
        }
    }

    public void writeRecord(Row row) throws IOException {
        try {
            JDBCUtils.setRecordToStatement(this.upload, this.typesArray, row);
            this.upload.addBatch();
        } catch (SQLException var3) {
            throw new RuntimeException("Preparation of JDBC statement failed.", var3);
        }

        ++this.batchCount;
        if (this.batchCount >= this.batchInterval) {
            this.flush();
        }

    }

    void flush() {
        try {
            this.upload.executeBatch();
            this.batchCount = 0;
        } catch (SQLException var2) {
            throw new RuntimeException("Execution of JDBC statement failed.", var2);
        }
    }

    int[] getTypesArray() {
        return this.typesArray;
    }

    public void close() throws IOException {
        if (this.upload != null) {
            this.flush();

            try {
                this.upload.close();
            } catch (SQLException var5) {
                LOG.info("JDBC statement could not be closed: " + var5.getMessage());
            } finally {
                this.upload = null;
            }
        }

        this.closeDbConnection();
    }

    public static MysqlUtil.MysqlUtilBuilder buildMysqlUtil() {
        return new MysqlUtil.MysqlUtilBuilder();
    }

    public static class MysqlUtilBuilder {
        private String username ;
        private String password ;
        private String drivername ;
        private String dbURL;
        private String query;
        private int batchInterval ;
        private int[] typesArray;

        protected MysqlUtilBuilder() {
            this.username = Constants.USER;
            this.password = Constants.PASS;
            this.drivername = Constants.DRIVER;
            this.dbURL = Constants.URL;
            this.batchInterval = Constants.BATCHSIZE;
        }

        public MysqlUtil.MysqlUtilBuilder setUsername(String username) {
            this.username = username;
            return this;
        }

        public MysqlUtil.MysqlUtilBuilder setPassword(String password) {
            this.password = password;
            return this;
        }

        public MysqlUtil.MysqlUtilBuilder setDrivername(String drivername) {
            this.drivername = drivername;
            return this;
        }

        public MysqlUtil.MysqlUtilBuilder setDBUrl(String dbURL) {
            this.dbURL = dbURL;
            return this;
        }

        public MysqlUtil.MysqlUtilBuilder setQuery(String query) {
            this.query = query;
            return this;
        }

        public MysqlUtil.MysqlUtilBuilder setBatchInterval(int batchInterval) {
            this.batchInterval = batchInterval;
            return this;
        }

        public MysqlUtil.MysqlUtilBuilder setSqlTypes(int[] typesArray) {
            this.typesArray = typesArray;
            return this;
        }

        public MysqlUtil finish() {
            if (this.username == null) {
                MysqlUtil.LOG.info("Username was not supplied.");
            }

            if (this.password == null) {
                MysqlUtil.LOG.info("Password was not supplied.");
            }

            if (this.dbURL == null) {
                throw new IllegalArgumentException("No database URL supplied.");
            } else if (this.query == null) {
                throw new IllegalArgumentException("No query supplied.");
            } else if (this.drivername == null) {
                throw new IllegalArgumentException("No driver supplied.");
            } else {
                return new MysqlUtil(this.username, this.password, this.drivername, this.dbURL, this.query, this.batchInterval, this.typesArray);
            }
        }
    }
}
