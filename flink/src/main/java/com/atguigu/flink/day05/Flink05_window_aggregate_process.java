package com.atguigu.flink.day05;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

public class Flink05_window_aggregate_process {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888).map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);

        WindowedStream<WaterSensor, String, TimeWindow> windowedDS = keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        windowedDS.aggregate(
                new AggregateFunction<WaterSensor, Integer, String>() {
                    @Override
                    public Integer createAccumulator() {
                        System.out.println("~~~~~初始化累加器~~~~~");
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {
                        System.out.println("~~~~~累加~~~~~");
                        return accumulator + value.vc;
                    }

                    @Override
                    public String getResult(Integer accumulator) {
                        System.out.println("~~~~~结果~~~~~");
                        return accumulator.toString();
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {
                        return null;
                    }
                },
                new ProcessWindowFunction<String, String, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<String, String, String, TimeWindow>.Context context, Iterable<String> elements, Collector<String> out) throws Exception {

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
