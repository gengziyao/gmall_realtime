package com.atguigu.flink.day08;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class Flink02_KeyedState_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);

        keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {

                    private ListState<Integer> vcListState;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        ListStateDescriptor<Integer> listState = new ListStateDescriptor<>("listState", Integer.class);
                        vcListState = getRuntimeContext().getListState(listState);
                    }

                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.vc;

                        vcListState.add(vc);

                        Iterable<Integer> it = vcListState.get();

                        ArrayList<Integer> vcList = new ArrayList<>();

                        for (Integer i : it) {
                            vcList.add(i);
                        }
                        //排序
                        vcList.sort((o1,o2)->o2 - o1);

                        if (vcList.size() > 3){
                            vcList.remove(3);
                        }

                        //输出
                        out.collect("传感器id：" + ctx.getCurrentKey() +"水位值最大的"+vcList.toString());

                        vcListState.update(vcList);

                    }
                }
        ).print();

        env.execute();
    }
}
