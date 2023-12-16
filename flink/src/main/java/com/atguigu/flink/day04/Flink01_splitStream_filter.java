package com.atguigu.flink.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 该案例演示了分流---filter
 *      经过分流处理之后，程序中有多条流存在
 *      当前应用的并行度，取决于每条流中的算子的最大并行度
 *
 */
public class Flink01_splitStream_filter {
    public static void main(String[] args) throws Exception {
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Integer> numDS = env.socketTextStream("hadoop102", 8888)
                .map(Integer::valueOf).setParallelism(2);

        SingleOutputStreamOperator<Integer> ds1 = numDS.filter(num -> num % 2 == 0).setParallelism(3);
        SingleOutputStreamOperator<Integer> ds2 = numDS.filter(num -> num % 2 != 0).setParallelism(4);

        ds1.print("偶数：");
        ds2.print("奇数：");

        env.execute();
    }
}
