package com.atguigu.flink.day03;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink11_agg_reduce {
    public static void main(String[] args) throws Exception {
        //准备环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.socketTextStream("hadoop102",8888)
                .map(
                        new WaterSensorMapFunction()
                ).keyBy(WaterSensor::getId)
                .reduce(
                        new ReduceFunction<WaterSensor>() {

                            //如果流中只有一条数据，
                            @Override
                            public WaterSensor reduce(WaterSensor waterSensor, WaterSensor t1) throws Exception {
                                System.out.println("value1" + waterSensor);
                                System.out.println("value2"+t1);
                                if (waterSensor.vc < t1.vc){
                                    waterSensor.vc = t1.vc;
                                }

                                return waterSensor ;
                            }
                        }
                ).print();

        env.execute();
    }
}
