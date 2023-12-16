package com.atguigu.flink.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

public class Flink04_connect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.setParallelism(1);

        env.disableOperatorChaining();

        DataStreamSource<String> stdDS1 = env.socketTextStream("hadoop102", 8888);

        SingleOutputStreamOperator<Integer> intDS2 = env.socketTextStream("hadoop102", 8889).map(Integer::valueOf);

        ConnectedStreams<String, Integer> connectDS = stdDS1.connect(intDS2);

        connectDS.map(
                new CoMapFunction<String, Integer, String>() {
                    @Override
                    public String map1(String value) throws Exception {
                        return "字符串流：" + value;
                    }

                    @Override
                    public String map2(Integer value) throws Exception {
                        return "数字流:" + value;
                    }
                }
        ).print();

        env.execute();
    }
}
