package com.atguigu.flink.day08;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReducingState;
import org.apache.flink.api.common.state.ReducingStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

public class Flink04_KeyedState_ReducingState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);

        keyedDS.process(

                new KeyedProcessFunction<String, WaterSensor, String>() {
                    private ReducingState<Integer> vcReducingState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ReducingStateDescriptor<Integer> reducingStateDescriptor = new ReducingStateDescriptor<Integer>(
                                "vcReducingState",
                                new ReduceFunction<Integer>() {
                                    @Override
                                    public Integer reduce(Integer value1, Integer value2) throws Exception {
                                        return value1 + value2;
                                    }
                                },
                                Integer.class);
                        vcReducingState = getRuntimeContext().getReducingState(reducingStateDescriptor);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.vc;

                        vcReducingState.add(vc);

                        out.collect("传感器id：" + ctx.getCurrentKey() +",水位累加值：" + vcReducingState.get());
                    }
                }

        ).print();

        env.execute();
    }
}
