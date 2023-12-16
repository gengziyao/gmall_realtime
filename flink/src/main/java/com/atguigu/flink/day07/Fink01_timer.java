package com.atguigu.flink.day07;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.TimerService;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Fink01_timer {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);


        SingleOutputStreamOperator<WaterSensor> wsDS = env
                .socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy
                                .<WaterSensor>forMonotonousTimestamps()
                                .withTimestampAssigner(
                                        new SerializableTimestampAssigner<WaterSensor>() {
                                            @Override
                                            public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                                return element.getTs();
                                            }
                                        }
                                )
                );

        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);

        keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        String currentKey = ctx.getCurrentKey();

                        Long timestamp = ctx.timestamp();

                        System.out.println("timeStamp:" + timestamp);

                        TimerService timerService = ctx.timerService();//定时服务 ·主要提供三项服务：1.注册定时器 2.删除定时器 3.获取处理时间/水位线

                        //注册一个事件时间定时器
                        timerService.registerEventTimeTimer(5);

                        //注册一个处理时间定时器
                        long currentProcessingTime = timerService.currentProcessingTime();
                        timerService.registerProcessingTimeTimer(currentProcessingTime  + 10000);

                        //删除定时器
                        //timerService.deleteEventTimeTimer(5);
                        //timerService.deleteProcessingTimeTimer(currentProcessingTime);

                        //获取水位线
                        System.out.println("水位线" + timerService.currentWatermark());

                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, WaterSensor, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
                        String currentKey = ctx.getCurrentKey();

                        out.collect("key=" + currentKey + "现在时间是" + timestamp + "定时器触发");
                    }
                }
        ).print();

        env.execute();

    }
}
