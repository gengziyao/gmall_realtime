package com.atguigu.flink.day05;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink04_window_process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);

        WindowedStream<WaterSensor, String, TimeWindow> windowedDS = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

/*        windowedDS.apply(new WindowFunction<WaterSensor, String, String, TimeWindow>() {
            @Override
            public void apply(String s, TimeWindow window, Iterable<WaterSensor> input, Collector<String> out) throws Exception {
                long count = input.spliterator().estimateSize();
                String windowStart = DateFormatUtils.format(window.getStart(), "yyyy-MM-dd HH:mm:ss");
                String windowEnd = DateFormatUtils.format(window.getEnd(), "yyyy-MM-dd HH:mm:ss");

                out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + input.toString());
            }
        }).print(">>>>");*/
        windowedDS.process(
                new ProcessWindowFunction<WaterSensor, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        long count = elements.spliterator().estimateSize();
                        String windowStart = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss");
                        String windowEnd = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss");

                        out.collect("key=" + s + "的窗口[" + windowStart + "," + windowEnd + ")包含" + count + "条数据===>" + elements.toString());
                    }
                }
        ).print("-~-~");

        env.execute();
    }
}
