package com.atguigu.flink.day08;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Map;

public class Flink03_KeyedState_MapState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);

        keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    private MapState<Integer,Integer> vcCountMapState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        MapStateDescriptor<Integer,Integer> stateProperties
                                = new MapStateDescriptor<>("vcCountMapState",Integer.class,Integer.class);
                        vcCountMapState = getRuntimeContext().getMapState(stateProperties);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        Integer vc = ws.vc;

                        if (vcCountMapState.contains(vc)){
                            vcCountMapState.put(vc,vcCountMapState.get(vc) + 1);
                        }else {
                            vcCountMapState.put(vc,1);
                        }


                        //遍历状态
                        StringBuilder outStr = new StringBuilder();
                        outStr.append("-----------------\n");
                        outStr.append("传感器-" + ctx.getCurrentKey()+"\n");

                        for (Map.Entry<Integer, Integer> entry : vcCountMapState.entries()) {
                            outStr.append("水位："+ entry.getKey() + ",出现次数"+ entry.getValue() + "\n");
                        }

                        outStr.append("-----------------\n");
                        out.collect(outStr.toString());

                    }
                }
        ).print();

        env.execute();
    }
}
