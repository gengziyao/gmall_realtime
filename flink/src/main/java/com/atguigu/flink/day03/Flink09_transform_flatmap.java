package com.atguigu.flink.day03;

import com.atguigu.flink.beans.WaterSensor;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink09_transform_flatmap {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<WaterSensor> wsDS = env.fromElements(
                new WaterSensor("s1", 1L, 10),
                new WaterSensor("s1", 2L, 20),
                new WaterSensor("s2", 2L, 20),
                new WaterSensor("s3", 3L, 30)
        );

        wsDS.flatMap(
                new FlatMapFunction<WaterSensor, String>() {
                    @Override
                    public void flatMap(WaterSensor waterSensor, Collector<String> collector) throws Exception {
                        if ("s1".equals(waterSensor.id)){
                            collector.collect(waterSensor.id+ ":" + waterSensor.vc);
                        } else if ("s2".equals(waterSensor.id)) {
                            collector.collect(waterSensor.id + ":" + waterSensor.vc);
                            collector.collect(waterSensor.id + ":" + waterSensor.ts);

                        }
                    }
                }
        ).print();

        env.execute();
    }
}
