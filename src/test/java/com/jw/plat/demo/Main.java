package com.jw.plat.demo;

import org.apache.flink.api.java.ExecutionEnvironment;

public class Main {
    public static void main(String[] args) throws Exception{
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        BatchUnion.proc(env, "d:/flink2/kk1-1", 1000);
        BatchUnion.proc(env, "d:/flink2/kk1-2", 1000);
        BatchUnion.proc(env, "d:/flink2/kk1-3", 1000);
        BatchUnion.proc(env, "d:/flink2/kk1-4", 1000);
        BatchUnion.proc(env, "d:/flink2/kk1-5", 1000);
        BatchUnion.proc(env, "d:/flink2/kk1-6", 1000);
        BatchUnion.proc(env, "d:/flink2/kk1-7", 1000);
        BatchUnion.proc(env, "d:/flink2/kk1-8", 1000);
        BatchUnion.proc(env, "d:/flink2/kk1-9", 1000);
        env.execute();
        env.clearJobListeners();
//        JavaGcCleanerWrapper
    }
}
