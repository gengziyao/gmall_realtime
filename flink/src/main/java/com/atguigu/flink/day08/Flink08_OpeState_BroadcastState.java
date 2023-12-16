package com.atguigu.flink.day08;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class Flink08_OpeState_BroadcastState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        SingleOutputStreamOperator<Integer> thresholdDS = env.socketTextStream("hadoop102", 8889)
                .map(Integer::valueOf);

        MapStateDescriptor<String, Integer> mapStateDescriptor
                = new MapStateDescriptor<String, Integer>("mapStateDescriptor",String.class, Integer.class);
        BroadcastStream<Integer> broadcastDS = thresholdDS.broadcast(mapStateDescriptor);//广播流 ,无法设置并行度，就是1

        BroadcastConnectedStream<WaterSensor, Integer> connectDS = wsDS.connect(broadcastDS);

        connectDS.process(
                new BroadcastProcessFunction<WaterSensor, Integer, String>() {
                    @Override
                    public void processElement(WaterSensor value, BroadcastProcessFunction<WaterSensor, Integer, String>.ReadOnlyContext ctx, Collector<String> out) throws Exception {
                        //处理主流数据
                        ReadOnlyBroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        Integer threshold = broadcastState.get("threshold");
                        threshold = threshold==null? 0 : threshold;

                        Integer vc = value.vc;

                        if (vc > threshold){
                            out.collect("当前水位："+ vc + "，超出了阈值" + threshold);
                        }

                    }

                    @Override
                    public void processBroadcastElement(Integer value, BroadcastProcessFunction<WaterSensor, Integer, String>.Context ctx, Collector<String> out) throws Exception {
                        //处理广播流数据
                        //获取广播状态
                        BroadcastState<String, Integer> broadcastState = ctx.getBroadcastState(mapStateDescriptor);
                        //将阈值数据放到广播状态中
                        broadcastState.put("threshold",value);

                    }
                }
        ).printToErr();

        env.execute();

    }
}
