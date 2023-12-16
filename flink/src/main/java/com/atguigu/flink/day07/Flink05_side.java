package com.atguigu.flink.day07;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

public class Flink05_side {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        OutputTag<String> warnTag = new OutputTag<>("warnTag");

        wsDS.process(
                new ProcessFunction<WaterSensor, String>() {
                    @Override
                    public void processElement(WaterSensor ws, ProcessFunction<WaterSensor, String>.Context ctx, Collector<String> out) throws Exception {
                        Integer vc = ws.vc;
                        if (vc == 10){
                            ctx.output(warnTag,ws.toString());
                        }else {
                            out.collect(ws.toString());
                        }

                    }
                }
        ).print();

        env.execute();
    }
}
