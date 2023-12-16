package com.atguigu.flink.day08;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

public class Flink01_KeyedState_ValueState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        KeyedStream<WaterSensor, String> keyedDS = wsDS.keyBy(ws -> ws.id);

/*  错误思路：
    keyedDS.process(
                new KeyedProcessFunction<String, WaterSensor, String>() {
                    Integer lastVc =0;  //多个并行度时会共用这个lastVc ,不同的分组也会共用这个lastVc
                    @Override
                    public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {

                        //获取当前传感器本次采集的水位线
                        Integer vc = ws.vc;
                        if (Math.abs(vc - lastVc) > 10){
                            out.collect("传感器id："+ ws.id + ",当前水位值" + vc + ",和上一次水位值" + lastVc+"相差大于10");
                        }

                        lastVc = vc;

                    }
                }
        ).print();*/
    keyedDS.process(
            new KeyedProcessFunction<String, WaterSensor, String>() {
                //声名状态
                private ValueState<Integer> lastVcState;

                //给状态进行初始化:在生命周期开始得时候

                //不能在状态声明的时候直接进行初始化，因为这个时候获取不到RuntimeContext
                // private ValueState<Integer> lastVcState
                // = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("lastVcState", Integer.class));
                @Override
                public void open(Configuration parameters) throws Exception {

                    //参数1:状态标记名称
                    //参数2:值状态中存放的元素类型
                    ValueStateDescriptor<Integer> valueStateDescriptor =
                            new ValueStateDescriptor<Integer>("lastVcState",Integer.class);


                    lastVcState = getRuntimeContext().getState(valueStateDescriptor);
                }

                @Override
                public void processElement(WaterSensor ws, KeyedProcessFunction<String, WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                    //获取本次水位值
                    Integer vc = ws.vc;
                    //获取上次水位值
                    Integer lastVc = lastVcState.value();
                    lastVc = lastVc == null? 0 : lastVc;
                    if (Math.abs(vc - lastVc) > 10){
                        out.collect("传感器id："+ ws.id + ",当前水位值" + vc + ",和上一次水位值" + lastVc + ",相差大于10");
                    }
                    // .update 将参数中给定的值 更新到状态中
                    lastVcState.update(vc);

                    //lastVcState.clear();

                }
            }
    ).print();

    env.execute();
    }
}
