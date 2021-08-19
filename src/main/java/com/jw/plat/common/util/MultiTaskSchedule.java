package com.jw.plat.common.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;

public class MultiTaskSchedule {
    LinkedBlockingQueue<TaskRequest> queue;
    private ExecutionEnvironment env;
    ScheduledExecutorService scheduledExecutorService;

    public MultiTaskSchedule(ExecutionEnvironment env) {
        this.env = env;
        this.queue = new LinkedBlockingQueue<TaskRequest>();
        this.scheduledExecutorService = Executors.newScheduledThreadPool(3);
    }

    public DataSet<Row> call(String biz) throws RuntimeException {
        TaskRequest request = new TaskRequest();
        request.biz = biz;
        CompletableFuture<DataSet<Row>> future = new CompletableFuture<>();
        request.future = future;
        queue.add(request);

        try {
            scheduledExecutorService.scheduleAtFixedRate(new Task(env, queue), 0, 3600*24, TimeUnit.SECONDS);
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @PostConstruct
    public void init() {


    }
}
