package com.atguigu.flink.day06;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class Flink06_join_window {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Tuple2<String, Integer>> ds1 = env
                .fromElements(
                        Tuple2.of("a", 1),
                        Tuple2.of("a", 2),
                        Tuple2.of("b", 3),
                        Tuple2.of("c", 4)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple2<String, Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value.f1)
                );


        SingleOutputStreamOperator<Tuple3<String, Integer,Integer>> ds2 = env
                .fromElements(
                        Tuple3.of("a", 1,1),
                        Tuple3.of("a", 11,1),
                        Tuple3.of("b", 2,1),
                        Tuple3.of("b", 12,1),
                        Tuple3.of("c", 14,1),
                        Tuple3.of("d", 15,1)
                )
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Tuple3<String, Integer,Integer>>forMonotonousTimestamps()
                                .withTimestampAssigner((value, ts) -> value.f1 )
                );

        ds1.join(ds2)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingEventTimeWindows.of(Time.milliseconds(10)))
                .apply(
                        new JoinFunction<Tuple2<String, Integer>, Tuple3<String, Integer, Integer>, String>() {
                            @Override
                            public String join(Tuple2<String, Integer> first, Tuple3<String, Integer, Integer> second) throws Exception {
                                return first + "-----" + second;
                            }
                        }
                ).print();

        env.execute();
    }
}
