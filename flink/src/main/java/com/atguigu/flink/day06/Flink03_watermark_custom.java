package com.atguigu.flink.day06;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink03_watermark_custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<WaterSensor> withWatermarkDS = wsDS.assignTimestampsAndWatermarks(
                WatermarkStrategy
                        .<WaterSensor>forGenerator(new WatermarkGeneratorSupplier<WaterSensor>() {
                            @Override
                            public WatermarkGenerator<WaterSensor> createWatermarkGenerator(Context context) {
                                return new MyWatermarkGenerator<WaterSensor>(2L);
                            }
                        })
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

        WindowedStream<WaterSensor, String, TimeWindow> windowedDS =
                keyedDS.window(TumblingEventTimeWindows.of(Time.milliseconds(10)));

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
        ).print(">>>");

        env.execute();
    }

}



 class MyWatermarkGenerator <T> implements WatermarkGenerator <T> {
    private long maxTs;
    private long delayTs;

    public MyWatermarkGenerator(Long delayTs){
        this.delayTs = delayTs;
        this.maxTs = Long.MIN_VALUE + delayTs + 1;
    }

    @Override
    public void onEvent(T event, long eventTimestamp, WatermarkOutput output) {

       maxTs =  Math.max(maxTs,eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {

        output.emitWatermark(new Watermark(maxTs - delayTs - 1));
    }
}
