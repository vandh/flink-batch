package com.jw.plat.modules.base;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;

public interface BaseExecute {
    public DataSet<Row> run(ExecutionEnvironment env);
}
