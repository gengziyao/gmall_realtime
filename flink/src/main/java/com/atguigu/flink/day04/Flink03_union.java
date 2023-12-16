package com.atguigu.flink.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink03_union {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> stdDS1 = env.socketTextStream("hadoop102", 8888);

        DataStreamSource<String> stdDS2 = env.socketTextStream("hadoop102", 8889);

        DataStreamSource<String> stdDS3 = env.fromElements("a", "b", "c");

        stdDS1.union(stdDS2,stdDS3).print();
        //stdDS1.union(stdDS2).union(stdDS3).print();

        env.execute();
    }
}
