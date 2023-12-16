package com.atguigu.flink.day03;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.LocalStreamEnvironment;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink01_env {
    public static void main(String[] args) throws Exception {
        //环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //本地执行环境
        //LocalStreamEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        //本地执行环境 带UI
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //远端执行环境
       // StreamExecutionEnvironment env =
        //        StreamExecutionEnvironment.createRemoteEnvironment("hadoop102", 8081, "F:\\Atguigu\\05_Code\\bigdata-parent\\flink\\target\\flink-1.0-SNAPSHOT.jar");

        //指定处理模式（流-默认、批）
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //还可以在提交应用的时候，通过参数指定模式 bin/flink run -Dexecution.runtime-mode=BATCH

        env.readTextFile("/opt/module/flink-1.17.0/wc.txt")
                .print();
        env.execute();
        //env.executeAsync(); 异步


 /*       StreamExecutionEnvironment env1 = StreamExecutionEnvironment.getExecutionEnvironment();
        env1.executeAsync();*/



    }
}
