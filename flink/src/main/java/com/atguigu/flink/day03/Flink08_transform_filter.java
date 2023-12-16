package com.atguigu.flink.day03;

import com.atguigu.flink.beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink08_transform_filter {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> wsDS = env.fromElements(
                new WaterSensor("s1", 1L, 10),
                new WaterSensor("s2", 2L, 20),
                new WaterSensor("s3", 3L, 30),
                new WaterSensor("s1", 4L, 40)
        );
        wsDS.filter(waterSensor -> "s1".equals(waterSensor.id))
                .print();
        env.execute();
    }
}
