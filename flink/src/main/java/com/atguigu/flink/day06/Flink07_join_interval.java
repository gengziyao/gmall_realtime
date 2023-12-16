package com.atguigu.flink.day06;

import com.atguigu.flink.beans.Dept;
import com.atguigu.flink.beans.Emp;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

public class Flink07_join_interval {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<Emp> empDS = env.socketTextStream("hadoop102", 8888)
                .map(s -> {
                    String[] fieldArr = s.split(",");
                    return new Emp(Integer.valueOf(fieldArr[0]), fieldArr[1],
                            Integer.valueOf(fieldArr[2]), Long.valueOf(fieldArr[3]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Emp>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Emp>() {
                                    @Override
                                    public long extractTimestamp(Emp element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        SingleOutputStreamOperator<Dept> deptDS = env.socketTextStream("hadoop102", 8889)
                .map(s -> {
                    String[] fieldArr = s.split(",");
                    return new Dept(Integer.valueOf(fieldArr[0]), fieldArr[1],
                             Long.valueOf(fieldArr[2]));
                })
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<Dept>forMonotonousTimestamps()
                                .withTimestampAssigner(new SerializableTimestampAssigner<Dept>() {
                                    @Override
                                    public long extractTimestamp(Dept element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                })
                );

        empDS.keyBy(Emp::getDeptno)

                .intervalJoin(deptDS.keyBy(Dept::getDeptno))
                .between(Time.milliseconds(-5),Time.milliseconds(5))
                .process(new ProcessJoinFunction<Emp, Dept, String>() {
                    @Override
                    public void processElement(Emp left, Dept right, ProcessJoinFunction<Emp, Dept, String>.Context ctx, Collector<String> out) throws Exception {
                        out.collect(left + "-----" + right);
                    }
                }).print(">>>");

        env.execute();

    }
}
