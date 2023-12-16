package com.atguigu.flink.day07;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

public class Flink02_window_watermark {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
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

        WindowedStream<WaterSensor, String, TimeWindow> windowedDS =
                keyedDS.window(TumblingEventTimeWindows.of(Time.milliseconds(10)));

        windowedDS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<WaterSensor, String, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        System.out.println("当前窗口单位水位线:" + context.currentWatermark());
                        long count = elements.spliterator().estimateSize();
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");
                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含"
                                + count + "条数据===>" + elements.toString());


                    }
                }
        ).process(
                        new ProcessFunction<String, String>() {
                            @Override
                            public void processElement(String value, ProcessFunction<String, String>.Context ctx, Collector<String> out) throws Exception {
                                //System.out.println("窗口处理完毕后的水位线："+ctx.timerService().currentWatermark());
                                out.collect("窗口处理完毕后的水位线："+ctx.timerService().currentWatermark());
                            }
                        }
                )
                .print();

        env.execute();
    }
}
