package com.atguigu.flink.day01;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 *
 */
public class Flink02_stream_bound {
    public static void main(String[] args) throws Exception {
        //TODO 1.指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //可切换批处理
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);


        //TODO 2.从指定的文件中读取数据
        DataStreamSource<String> textDS = env.readTextFile("F:\\Atguigu\\05_Code\\bigdata-parent\\flink\\input\\wc.txt");

        //TODO 3. 将读取的数据进行转换   封装为二元组
        SingleOutputStreamOperator<Tuple2<String, Long>> tupleDS = textDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] wordArr = s.split(" ");
                for (String string : wordArr) {
                    collector.collect(Tuple2.of(string, 1L));
                }
            }
        });

        //TODO 4.分组
        KeyedStream<Tuple2<String, Long>, Tuple> keyDS = tupleDS.keyBy(0);

        //TODO 5.聚合
        SingleOutputStreamOperator<Tuple2<String, Long>> sum = keyDS.sum(1);

        sum.print();

        //TODO 执行
        env.execute();

    }
}
