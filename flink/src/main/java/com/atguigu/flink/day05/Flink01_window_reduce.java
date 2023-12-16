package com.atguigu.flink.day05;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.awt.datatransfer.SystemFlavorMap;

public class Flink01_window_reduce {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

/*        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(new KeySelector<WaterSensor, String>() {
            @Override
            public String getKey(WaterSensor ws) throws Exception {
                return ws.id;
            }
        });*/

        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);

        //KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(WaterSensor::getId);

        WindowedStream<WaterSensor, String, TimeWindow> windowedDS =
                keyedDS.window(TumblingProcessingTimeWindows.of(Time.seconds(10)));

        windowedDS.reduce(
                new ReduceFunction<WaterSensor>() {
                    @Override
                    public WaterSensor reduce(WaterSensor value1, WaterSensor value2) throws Exception {
                        System.out.println("中间结果:"+ value1);
                        System.out.println("新建数据:"+ value2);

                        return new WaterSensor(value1.id, System.currentTimeMillis(),value1.vc+value2.vc);
                    }
                }
        ).print(">>>");

        env.execute();
    }
}
