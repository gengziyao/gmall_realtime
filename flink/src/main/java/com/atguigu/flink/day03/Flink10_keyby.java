package com.atguigu.flink.day03;

import com.atguigu.flink.beans.WaterSensor;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 该案例演示了keyby操作以及聚合
 *      keyby作用：是对数据进行分组(重分区)
 *      keyby不算一个转换算子，只是一个分组操作
 *      flink提供了一些聚合算子sum/min/max/minBy/maxBy/reduce，但是这些算子在使用前，必须先进行keyBy分组 转为KeyedStream结构数据
 *      底层调了两次hash算法，一次hash 一次murmurHush来保证数据在不同分区的均匀分布
 */
public class Flink10_keyby {
    public static void main(String[] args) throws Exception {
        //keyBy : 对数据进行分组 （重分区）
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        DataStreamSource<WaterSensor> wsDS = env.fromElements(
                new WaterSensor("s1", 1L, 10),
                new WaterSensor("s1", 2L, 30),
                new WaterSensor("s1", 3L, 20),
                new WaterSensor("s1", 4L, 40)
        );
        KeyedStream<WaterSensor, String> keyedBy = wsDS.keyBy(WaterSensor::getId);
        //keyedBy.print();

        //keyedBy.max("vc").print();
        DataStreamSink<WaterSensor> vc = keyedBy.maxBy("vc").print();

        env.execute();
    }
}
