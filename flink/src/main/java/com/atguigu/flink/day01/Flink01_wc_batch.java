package com.atguigu.flink.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.FlatMapOperator;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class Flink01_wc_batch {
    public static void main(String[] args) throws Exception {
        //TODO 1.指定批处理环境
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        //TODO 2.从指定的文件中读取数据
        DataSource<String> textDS = env.readTextFile("F:\\Atguigu\\05_Code\\bigdata-parent\\flink\\input\\wc.txt");

        //TODO 3. 将读取的数据进行转换   封装为二元组
        FlatMapOperator<String, Tuple2<String, Long>> tupleWordDS = textDS.flatMap(new FlatMapFunction<String, Tuple2<String, Long>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String, Long>> collector) throws Exception {
                String[] wordArr = s.split(" ");
                for (String string : wordArr) {
                    collector.collect(Tuple2.of(string, 1L));
                }
            }
        });

        //TODO 4.分组
        UnsortedGrouping<Tuple2<String, Long>> groupDS = tupleWordDS.groupBy(0);

        //TODO 5.聚合
        AggregateOperator<Tuple2<String, Long>> sumDS = groupDS.sum(1);

        sumDS.print();
    }
}
