package com.atguigu.flink.day07;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class Flink03_topn_1 {
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
                                                return element.getTs() * 1000;
                                            }
                                        }
                                )
                );

        AllWindowedStream<WaterSensor, TimeWindow> windowedAll =
                wsDS.windowAll(SlidingEventTimeWindows.of(Time.seconds(10),Time.seconds(5)));

        windowedAll.process(
                new ProcessAllWindowFunction<WaterSensor, String, TimeWindow>() {
                    @Override
                    public void process(ProcessAllWindowFunction<WaterSensor, String, TimeWindow>.Context context, Iterable<WaterSensor> elements, Collector<String> out) throws Exception {
                        Map<Integer,Integer> vcCountMap = new HashMap<>();

                        for (WaterSensor element : elements) {
                            Integer vc = element.getVc();
                            //判断vc是否已存在
                            if (vcCountMap.containsKey(vc)){
                                vcCountMap.put(vc,vcCountMap.get(vc)+1);
                            }else {
                                vcCountMap.put(vc, 1);
                            }
                        }

                        //排序
                        ArrayList<Tuple2<Integer, Integer>> vcCountList = new ArrayList<>();

                        Set<Map.Entry<Integer, Integer>> entrySet = vcCountMap.entrySet();
                        for (Map.Entry<Integer, Integer> entry : entrySet) {
                            vcCountList.add(Tuple2.of(entry.getKey(),entry.getValue()));
                        }

                        vcCountList.sort((o1,o2)->o2.f1-o1.f1);
                        //取前两名
                        StringBuilder outStr = new StringBuilder();
                        outStr.append("-----------------------\n");

                        for (int i =0;i<Math.min(2,vcCountList.size());i++){
                            Tuple2<Integer, Integer> vcCountTuple2 = vcCountList.get(i);
                            Integer vc = vcCountTuple2.f0;
                            Integer count = vcCountTuple2.f1;
                            outStr.append("Top"+(i+1)+"\n");
                            outStr.append("VC:"+ vc + ",Count:" + count+ "\n");
                            String stt = DateFormatUtils.format(context.window().getStart(), "yyyy-MM-dd HH:mm:ss.SSS");
                            String edt = DateFormatUtils.format(context.window().getEnd(), "yyyy-MM-dd HH:mm:ss.SSS");

                            outStr.append("窗口开始时间:" + stt +"-------"+"窗口结束时间" +edt);



                        }

                        out.collect(outStr.toString());
                    }

                }
        ).print(">>>");

        env.execute();
    }
}
