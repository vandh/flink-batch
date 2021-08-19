package com.jw.plat.common.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.types.Row;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class TaskRequest {
    public String biz;
    public CompletableFuture<DataSet<Row>> future;
}
