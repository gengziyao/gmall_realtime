package com.atguigu.flink.day06;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink02_watermark_BoundedOut {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        //TODO 指定Watermark的生成策略，并提取事件时间字段
        SingleOutputStreamOperator<WaterSensor> withWatermarkDS = wsDS.assignTimestampsAndWatermarks(
                //TODO  Flink提供了两种默认的水位线生成策略：单调递增（有序） 有界乱序  ，都是周期性生成的
                WatermarkStrategy
                        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofMillis(2))  //指定生成策略为有界乱序
                        .withTimestampAssigner(
                                new SerializableTimestampAssigner<WaterSensor>() {
                                    @Override
                                    public long extractTimestamp(WaterSensor element, long recordTimestamp) {
                                        return element.getTs();
                                    }
                                }
                        )
        );
        KeyedStream<WaterSensor, String> keyedDS = withWatermarkDS.keyBy(ws -> ws.id);

        WindowedStream<WaterSensor, String, TimeWindow> windowedDS = keyedDS.window(TumblingEventTimeWindows.of(Time.milliseconds(10)));

        windowedDS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long count = elements.spliterator().estimateSize();
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含"
                                + count + "条数据===>" + elements.toString());


                    }
                }
        ).print();

        env.execute();
    }
}
