# DW大数据项目

​	利用FLINK批处理功能，处理TB级大数据文件，多文件内外连接，垃圾数据过滤，最后导入关系数据库mysql的全套解决方案。

​	属原创项目，创造性的解决了mysql大数据文件导入导出，大数据量关联查询等低效问题，几何级提升了大文件处理速度，大量节约磁盘空间，也取得flink批处理应用场景的突破。

# 一、系统架构

![sa](src\main\resources\pic\sa.png)

# 二、业务流程

## sftp

erp系统定期通过sftp服务器推送文件。

文件存放地址：

- 全量数据文件：1~5月份，/rtp_dw集群/xx.238.25.191/data/history
- 增量数据文件：每天推送，包括前5天的数据，/data/day/日期目录

文件分类：

​	共五个模块，分别是：GL、APPRE、APSTD、AR、RA

​	每个模块分1~8个批次，每个批次19个文件；

## 第1步

3个模块、8个批次，共3*8*19=259个文件；

通过Flink集群跑批，跑批程序详本项目

产生结果文件共3*8=24个

## 第2步

结果文件通过mysql Load命令导入到表：

全量文件进24张全量表：

gl_voucher_info_1 ~ 8;

ap_pre_voucher_info_1 ~ 8;

ap_std_voucher_info_1 ~ 8;

每张全量表关键字段：

- 主键：id 自增；
- 普通索引：批batch_id、头ae_header_id、行ae_line_num

- 日期字段：create_date

增量文件进24张增量表，没有主键，也没有索引，没有create_date：

- add_gl_voucher_info_1 ~ 8;

- add_ap_pre_voucher_info_1 ~ 8;

- add_ap_std_voucher_info_1 ~ 8;

## 第3步

24张表进2张业务表，然后通过规则更新comp_id字段的值：

全量表：手动执行sql，上线之前已经完成。

增量表：每天自动执行load命令载入数据，通过设置唯一索引，自动去掉重复数据；

2张业务表关键字段：

- 主键：id 自增；
- 唯一索引：批batch_id、头ae_header_id、行ae_line_num

- 日期字段：create_date，增量表没有

## 第4步

​	24张增量表插入24张全量表，每条记录设置插入记录的日期字段为文件名的日期，插入前先删除，24张全量表只需要建普通索引，不需要唯一索引，插入语句：

delete from gl_voucher_info_1 where create_date=’2021-07-14’;

insert into gl_voucher_info_1 select *,’2021-07-14’ from add_gl_voucher_info_1;

## 第5步

2张业务增量表数据导入到2张业务全量表，需要考虑重复数据和插入日期，插入前先删除，sql语句为：

delete from rtp_gl_voucher_info where create_date=’2021-07-14’;

insert into rtp_gl_voucher_info 

select *, ’2021-07-14’ from add_rtp_gl_voucher_info where (batch_id, ae_header_id, ae_line_num) not in (select batch_id, ae_header_id, ae_line_num from rtp_gl_voucher_info);

## 第6步

2张业务增量表数据从dw接口库导入到rtp业务库中的2张业务全量表，需要考虑重复数据和插入日期，插入前先删除，sql语句为：

1）dw库获取数据结果集：

select *, ’2021-07-14’ from add_rtp_gl_voucher_info where (batch_id, ae_header_id, ae_line_num) not in (select batch_id, ae_header_id, ae_line_num from rtp_gl_voucher_info);

2）rtp业务库删除当天数据：

delete from rtp_gl_voucher_info where create_date=’2021-07-14’;

3）插入业务库

insert into rtp_gl_voucher_info as 结果集;

# 三、自动跑批

​	对于taskManager pod，因为每次跑后pod的内存没有清理，导致下次任务可能会失败。所以可以用CronJob清理，策略是每天凌晨3:50重启动一次，23小时后pod回收；

​	对于JobManager pod，也使用CronJob调度，策略是每天凌晨4:00点重启动一次，启动后立即自动执行/data/flinktool/run.sh脚本，23小时后pod回收。

​	run.sh脚本如下：

```
#!/bin/bash
cd /data/day
dir=/data/day/D_*
dwfiles=$(ls ${dir})
for sfile in ${dwfiles}
do
    sfile=${sfile##*/}
    newDir=${sfile:2:8}
    if [[ ${newDir} -ge "20200101" && ${newDir} -le "20310715" && ! -d ${newDir} ]]; then
          #echo "make dir - ${newDir}"
          mkdir ${newDir}
		  mv D_${newDir}_* ${newDir}
    fi
done
echo "no.1 task GL 1,2"
flink run -c com.jw.plat.Main2 /data/flinktool/data-flink-1.0-SNAPSHOT.jar  /data/day/20210730 1-2 GBK - GL 2 FILE /data/day/result_add/$(date -d last-day +%Y%m%d)
echo "no.2 task GL 5,6"
flink run -c com.jw.plat.Main2 /data/flinktool/data-flink-1.0-SNAPSHOT.jar  /data/day/20210730 5-6 GBK - GL 2 FILE /data/day/result_add/$(date -d last-day +%Y%m%d)
...
```

# 四、flink集群部署

flink集群采用k8s部署，其中JobManager一个节点，2C8G；TaskManager二个节点，8C64G；

## 1. Dockerfile

```
FROM centos:7
MAINTAINER simon
RUN yum install java-1.8.0-openjdk -y
ARG FLINK_VERSION=1.12.4
ARG SCALA_VERSION=2.11
ARG FLINK_TAR_NAME=flink-${FLINK_VERSION}-bin-scala_${SCALA_VERSION}.tgz
ENV FLINK_HOME=/flink-${FLINK_VERSION}
ADD ${FLINK_TAR_NAME} /
RUN useradd flink
RUN mkdir /data
RUN chown flink:flink -R /data
RUN chmod 777 /data
VOLUME $FLINK_HOME/conf
```

## 2. JobManager.yaml

```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-jobmanager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink
      component: jobmanager
  template:
    metadata:
      labels:
        app: flink
        component: jobmanager
    spec:
      nodeName: 192.168.0.1
      containers:
        - name: jobmanager
          image: harbor.dcos.xixian.unicom.local/rtp-ccr/flink:1080
          workingDir: /opt/flink
          command: ["/bin/bash", "-c", "$FLINK_HOME/bin/jobmanager.sh start;\
          while :;
          do
            if [[ -f $(find log -name '*jobmanager*.log' -print -quit) ]];
              then tail -f -n +1 log/*jobmanager*.log;
            fi;
          done"]
          ports:
            - containerPort: 6123
              name: rpc
            - containerPort: 6124
              name: blob
            - containerPort: 8081
              name: webui
            # 需要指定该端口，否则将随机生成，导致TaskManager与JobManager通信异常
            - containerPort: 34560
              name: ha
            - containerPort: 34561
              name: query
          resources:
            requests:
              cpu: 2
              memory: 4Gi
            limits:
              cpu: 2
              memory: 3Gi
          livenessProbe:
            tcpSocket:
              port: 6123
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: flink-data-volume
              mountPath: /data
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
        - name: flink-data-volume
          persistentVolumeClaim:
            claimName: cfs-pvc
            readOnly: false
        - name: flink-config-volume
          configMap:
            name: flink-cm
            items:
              - key: flink-conf.yaml
                path: flink-conf.yaml
              - key: log4j.properties
                path: log4j.properties
              - key: log4j-cli.properties
                path: log4j-cli.properties
```

## 3. taskManager.yaml

```
apiVersion: apps/v1
kind: Deployment
metadata:
  name: flink-taskmanager
spec:
  replicas: 1
  selector:
    matchLabels:
      app: flink
      component: taskmanager
  template:
    metadata:
      labels:
        app: flink
        component: taskmanager
    spec:
      nodeName: 192.168.0.4
      containers:
        - name: taskmanager
          image: harbor.dcos.xixian.unicom.local/rtp-ccr/flink:1080
          workingDir: /opt/flink
          command: ["/bin/bash", "-c", "$FLINK_HOME/bin/taskmanager.sh start; \
          while :;
          do
            if [[ -f $(find log -name '*taskmanager*.log' -print -quit) ]];
              then tail -f -n +1 log/*taskmanager*.log;
            fi;
          done"]
          ports:
            - containerPort: 6122
              name: rpc
            - containerPort: 6125
              name: query-state
          resources:
            requests:
              cpu: 2.0
              memory: 60Gi
            limits:
              cpu: 4
              memory: 60Gi
          livenessProbe:
            tcpSocket:
              port: 6122
            initialDelaySeconds: 30
            periodSeconds: 60
          volumeMounts:
            - name: flink-config-volume
              mountPath: /opt/flink/conf
            - name: flink-data-volume
              mountPath: /data
          securityContext:
            runAsUser: 9999  # refers to user _flink_ from official flink image, change if necessary
      volumes:
        - name: flink-data-volume
          persistentVolumeClaim:
            claimName: cfs-pvc
            readOnly: false
        - name: flink-config-volume
          configMap:
            name: flink-cm
            items:
              - key: flink-conf.yaml
                path: flink-conf.yaml
              - key: log4j.properties
                path: log4j.properties
              - key: log4j-cli.properties
                path: log4j-cli.properties
```

## 4. flink-conf.yaml

注意jvm调优要和本地集群节点资源配置相适应。

```
apiVersion: v1
kind: ConfigMap
metadata:
  name: flink-cm
  labels:
    app: flink
data:
  flink-conf.yaml: |+
    jobmanager.rpc.address: flink-jobmanager
    taskmanager.numberOfTaskSlots: 1
    blob.server.port: 6124
    jobmanager.rpc.port: 6123
    taskmanager.rpc.port: 6122
    jobmanager.heap.size: 3096m
    # taskmanager.memory.process.size: 38000m
    taskmanager.memory.task.heap.size: 58000m
    taskmanager.memory.managed.size: 55000m
    env.java.opts: -XX:NewRatio=1 -XX:SurvivorRatio=8 -Xss256k -XX:MetaspaceSize=128M -XX:MaxMetaspaceSize=128M -XX:MaxTenuringThreshold=0 -XX:ParallelGCThreads=2 -XX:MaxGCPauseMillis=600000 -XX:GCTimeRatio=5 -XX:+UseG1GC
    state.backend: filesystem
    state.checkpoints.dir: file:///data/flink/checkpoints
    # 设置作业元数据的存储位置
    high-availability.storageDir: file:///data/flink/ha
    high-availability.jobmanager.port: 34560
    metrics.internal.query-service.port: 34561
```

