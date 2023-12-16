package com.atguigu.flink.day07;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.commons.lang3.time.DateFormatUtils;
import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.util.*;

public class Flink04_topn_2 {
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

        KeyedStream<WaterSensor, Integer> keyedDS = wsDS.keyBy(ws -> ws.vc);

        WindowedStream<WaterSensor, Integer, TimeWindow> windowedDS =
                keyedDS.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        //对窗口中vc对应的次数进行 增量+全量 聚合
        SingleOutputStreamOperator<Tuple3<Integer, Integer, Long>> aggDS = windowedDS.aggregate(
                new MyAggFunction(),
                //哪个vc  出现多少次  属于哪个窗口
                new MyProFunction()

        );
        KeyedStream<Tuple3<Integer, Integer, Long>, Long> windowEndKeyedDS = aggDS.keyBy(t -> t.f2);

        //排序
        windowEndKeyedDS
                .process(new MyTopnFunction(2))
                .print();

        env.execute();
    }


    public static class MyAggFunction implements AggregateFunction<WaterSensor, Integer, Integer> {
        @Override
        public Integer createAccumulator() {
            return 0;
        }

        @Override
        public Integer add(WaterSensor value, Integer accumulator) {
            return accumulator + 1;
        }

        @Override
        public Integer getResult(Integer accumulator) {
            return accumulator;
        }

        @Override
        public Integer merge(Integer a, Integer b) {
            return null;
        }
    }

   public static class MyProFunction extends ProcessWindowFunction<Integer, Tuple3<Integer, Integer, Long>,Integer, TimeWindow> {

       @Override
       public void process(Integer key, Context context,
                           Iterable<Integer> elements, Collector<Tuple3<Integer, Integer, Long>> out) throws Exception {
           Integer count = elements.iterator().next();
           long end = context.window().getEnd();
           out.collect(Tuple3.of(key,count,end));
       }
   }


   public static class MyTopnFunction extends KeyedProcessFunction<Long,Tuple3<Integer,Integer,Long>,String> {

       private Map<Long, List<Tuple3<Integer, Integer, Long>>> vcCountWindowMap = new HashMap();

       private int topnCount;

       public MyTopnFunction(int topnCount) {
           this.topnCount = topnCount;
       }

       @Override
       public void processElement(Tuple3<Integer, Integer, Long> vcEnd,Context ctx, Collector<String> out) throws Exception {
           Long windowEnd = ctx.getCurrentKey();

           if (vcCountWindowMap.containsKey(windowEnd)) {
               vcCountWindowMap.get(windowEnd).add(vcEnd);
           } else {
               ArrayList<Tuple3<Integer, Integer, Long>> vcList = new ArrayList<>();
               vcList.add(vcEnd);
               vcCountWindowMap.put(windowEnd, vcList);
           }

           //为了让上游同一窗口的数据都收集齐，等1ms ，1ms后执行事件时间定时器
          // ctx.timerService().registerEventTimeTimer(windowEnd + 1);
       }

       @Override
       public void onTimer(long timestamp, KeyedProcessFunction<Long, Tuple3<Integer, Integer, Long>, String>.OnTimerContext ctx, Collector<String> out) throws Exception {
           Long windowEnd = ctx.getCurrentKey();
           List<Tuple3<Integer, Integer, Long>> vcCWList = vcCountWindowMap.get(windowEnd);

           vcCWList.sort((o1, o2) -> o2.f1 - o1.f1);


           //取前两名
           StringBuilder outStr = new StringBuilder();
           outStr.append("-----------------------\n");

           for (int i = 0; i < Math.min(topnCount, vcCWList.size()); i++) {
               Tuple3<Integer, Integer, Long> vcCountTuple3 = vcCWList.get(i);
               Integer vc = vcCountTuple3.f0;
               Integer count = vcCountTuple3.f1;
               outStr.append("Top" + (i + 1) + "\n");
               outStr.append("VC:" + vc + ",Count:" + count + "\n");
               outStr.append("窗口结束时间=" + windowEnd + "\n");
           }
           vcCWList.clear();

           out.collect(outStr.toString());
       }
   }
}