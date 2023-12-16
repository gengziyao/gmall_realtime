package com.atguigu.flink.day03;


import com.atguigu.flink.beans.WaterSensor;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 该案例演示了转换算子--map
 * 需求：提取流中WaterSensor中的id字段的功能
 */

public class Flink07_transform_map {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<WaterSensor> wsDS = env.fromElements(
                new WaterSensor("sensor_1", 1L, 1),
                new WaterSensor("sensor_2", 2L, 2)
        );

/*        wsDS.map(
                new MapFunction<WaterSensor,String>() {
                    @Override
                    public String map(WaterSensor waterSensor) throws Exception {
                        return waterSensor.id;
                    }
                }
        ).print();*/

        //wsDS.map(ws -> ws.id).print();

/*        wsDS.map(
                WaterSensor::getId
        ).print();*/

        //抽取专门的类 实现MapFunction
        wsDS.map(new MyMap()).print();

        env.execute();
    }

}

class MyMap implements MapFunction<WaterSensor,String>{
    @Override
    public String map(WaterSensor waterSensor) throws Exception {
        return waterSensor.id;
    }

}
