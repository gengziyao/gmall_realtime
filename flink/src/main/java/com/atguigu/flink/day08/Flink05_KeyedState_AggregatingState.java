package com.atguigu.flink.day08;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.AggregatingState;
import org.apache.flink.api.common.state.AggregatingStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink05_KeyedState_AggregatingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);

        keyedDS.process(

                new KeyedProcessFunction<String, WaterSensor, String>() {
                    private AggregatingState<Integer, Double> vcAggregatingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double> stateDescriptor
                                = new AggregatingStateDescriptor<Integer, Tuple2<Integer, Integer>, Double>("vcAggregatingState",
                                new AggregateFunction<Integer, Tuple2<Integer, Integer>, Double>() {
                                    @Override
                                    public Tuple2<Integer, Integer> createAccumulator() {
                                        return Tuple2.of(0,0);
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> add(Integer value, Tuple2<Integer, Integer> accumulator) {
                                        return Tuple2.of(accumulator.f0 + value,accumulator.f1 + 1);
                                    }

                                    @Override
                                    public Double getResult(Tuple2<Integer, Integer> accumulator) {
                                        return accumulator.f0 * 1D/accumulator.f1;
                                    }

                                    @Override
                                    public Tuple2<Integer, Integer> merge(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
                                        return null;
                                    }
                                },
                                Types.TUPLE(Types.INT, Types.INT));
                        vcAggregatingState = getRuntimeContext().getAggregatingState(stateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor value, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        Integer vc = value.vc;

                        vcAggregatingState.add(vc);
                        out.collect("传感器id"+ ctx.getCurrentKey() + ",平均水位："+vcAggregatingState.get());
                    }
                }
                    ).print();

        env.execute();
    }
}
