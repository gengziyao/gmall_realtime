package com.atguigu.flink.day05;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

public class Flink02_window_aggregate {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());


        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);


        WindowedStream<WaterSensor, String, TimeWindow> windowedDS =
                keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        windowedDS.aggregate(
                new AggregateFunction<WaterSensor, Integer, String>() {
                    @Override
                    public Integer createAccumulator() {//初始化累加器
                        System.out.println("~~~~~初始化累加器~~~~~");
                        return 0;
                    }

                    @Override
                    public Integer add(WaterSensor value, Integer accumulator) {//当窗口中有数据进来时进行累加
                        System.out.println("~~~~~累加add~~~~~");
                        return accumulator + value.vc;
                    }

                    @Override
                    public String getResult(Integer accumulator) {
                        System.out.println("~~~~~获取结果~~~~");
                        return accumulator.toString();
                    }

                    @Override
                    public Integer merge(Integer a, Integer b) {//会话窗口会用到
                        System.out.println("~~~~~merge~~~~~");
                        return null;
                    }
                }
        ).print();

        env.execute();
    }
}
