package com.jw.plat.common.util;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.concurrent.LinkedBlockingQueue;

public class Task implements Runnable{
    private ExecutionEnvironment env;
    LinkedBlockingQueue<TaskRequest> queue;

    public Task(ExecutionEnvironment env, LinkedBlockingQueue<TaskRequest> queue) {
        this.env = env;
        this.queue = queue;
    }

    @Override
    public void run() {
        int size = queue.size();
        if (size == 0) {
            return;
        }
        ArrayList<TaskRequest> requests = new ArrayList<>();
        HashMap<String, DataSet<Row>> responseMap = new HashMap<>();
        for (int i = 0; i < size; i++) {
            TaskRequest request = queue.poll();
            requests.add(request);
        }

        for (TaskRequest request : requests) {
            responseMap.put(request.biz, Constants.BIZMAP.get(request.biz).f0.run(env));
          }

        for (TaskRequest request : requests) {
            DataSet<Row> result = responseMap.get(request.biz);
            request.future.complete(result);
        }

    }
}
