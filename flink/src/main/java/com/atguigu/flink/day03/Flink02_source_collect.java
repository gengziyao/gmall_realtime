package com.atguigu.flink.day03;


import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

//演示源算子  --从集合中读数据
public class Flink02_source_collect {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //DataStreamSource<Integer> colDS = env.fromCollection(Arrays.asList(1, 2, 3, 4));
        DataStreamSource<Integer> colDS = env.fromElements(1, 2, 3, 4);
        colDS.print();

        env.execute();
    }
}
