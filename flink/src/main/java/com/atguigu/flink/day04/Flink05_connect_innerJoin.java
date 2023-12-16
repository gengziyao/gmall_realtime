package com.atguigu.flink.day04;

import org.apache.commons.collections.map.HashedMap;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedCoProcessFunction;
import org.apache.flink.util.Collector;

import java.util.*;

public class Flink05_connect_innerJoin {
    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

                DataStreamSource<Tuple2<Integer, String>> ds1 = env.fromElements(
                Tuple2.of(1, "a1"),
                Tuple2.of(1, "a2"),
                Tuple2.of(2, "b"),
                Tuple2.of(3, "c")
        );
        DataStreamSource<Tuple3<Integer, String, Integer>> ds2 = env.fromElements(
                Tuple3.of(1, "aa1", 1),
                Tuple3.of(1, "aa2", 2),
                Tuple3.of(2, "bb", 1),
                Tuple3.of(3, "cc", 1)
        );

/*        KeyedStream<Tuple2<Integer, String>, Integer> keyedDS1 = ds1.keyBy(t1 -> t1.f0);
        KeyedStream<Tuple3<Integer, String,Integer>, Integer> keyedDS2 = ds2.keyBy(t2 -> t2.f0);
        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectDS = keyedDS1.connect(keyedDS2);*/


        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> connectDS = ds1.connect(ds2);

        ConnectedStreams<Tuple2<Integer, String>, Tuple3<Integer, String, Integer>> keyedDS = connectDS.keyBy(
                t1 -> t1.f0,
                t2 -> t2.f0
        );

        //对连接后的数据进行处理
        SingleOutputStreamOperator<String> processDS = keyedDS.process(
                new KeyedCoProcessFunction<Integer, Tuple2<Integer, String>, Tuple3<Integer, String, Integer>, String>() {
                    private Map<Integer, List<Tuple2<Integer, String>>> ds1Cache = new HashMap<>();
                    private Map<Integer, List<Tuple3<Integer, String, Integer>>> ds2Cache = new HashMap<>();

                    @Override
                    public void processElement1(Tuple2<Integer, String> value, Context ctx, Collector<String> out) throws Exception {
                        Integer key = ctx.getCurrentKey();
                        //当第一条数据来的时候，先将其放入缓存中缓存起来
                        if (ds1Cache.containsKey(key)) {
                            ds1Cache.get(key).add(value);
                        } else {
                            ArrayList<Tuple2<Integer, String>> ds1List = new ArrayList<>();
                            ds1List.add(value);
                            ds1Cache.put(key, ds1List);
                        }
                        //用当前处理的数据与另一条流中已经缓存的数据进行关联
                        if (ds2Cache.containsKey(key)) {
                            for (Tuple3<Integer, String, Integer> e2 : ds2Cache.get(key)) {
                                out.collect("s1:" + value + "<--------->s2:" + e2);
                            }

                        }

                    }


                    @Override
                    public void processElement2(Tuple3<Integer, String, Integer> value, Context ctx, Collector<String> out) throws Exception {

                        Integer key = ctx.getCurrentKey();
                        //当第一条数据来的时候，先将其放入缓存中缓存起来
                        if (ds2Cache.containsKey(key)) {
                            ds2Cache.get(key).add(value);
                        } else {
                            ArrayList<Tuple3<Integer, String, Integer>> ds2List = new ArrayList<>();
                            ds2List.add(value);
                            ds2Cache.put(key, ds2List);
                        }
                        //用当前处理的数据与另一条流中已经缓存的数据进行关联
                        if (ds1Cache.containsKey(key)) {
                            for (Tuple2<Integer, String> e2 : ds1Cache.get(key)) {
                                out.collect("s1:" + e2 + "<--------->s2:" + value);
                            }

                        }
                    }
                }
        );

        processDS.print(">>>");

        env.execute();

    }
}
