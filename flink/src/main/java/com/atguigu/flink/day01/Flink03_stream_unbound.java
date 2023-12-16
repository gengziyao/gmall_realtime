package com.atguigu.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink03_stream_unbound {
    public static void main(String[] args) throws Exception {
        //TODO 1.指定流处理环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 2.从指定的网络端口读取数据
        env.socketTextStream("hadoop102", 8888)

        //TODO 3. 将读取的数据进行转换   封装为二元组
        .flatMap(
                 (String s,Collector<Tuple2<String,Long>> collector) -> {//存在泛型擦除问题

                    String[] wordArr = s.split(" ");
                    for (String string : wordArr) {
                        collector.collect(Tuple2.of(string, 1L));
                    }

                }
        )//.returns(new TypeHint<Tuple2<String, Long>>() {})
           .returns(Types.TUPLE(Types.STRING,Types.LONG))
        //TODO 4.按照单词分组
        .keyBy(word -> word.f0)

        //TODO 5.聚合
        .sum(1).print();

        //TODO 执行
        env.execute();
    }
}
