package com.atguigu.flink.day04;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.SideOutputDataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;


/**
 * 该案例演示了分流--侧输出流
 * 需求：将WaterSensor按照Id类型进行分流。
 */
public class Flink02_splitStream_side {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        OutputTag<WaterSensor> s1Tag = new OutputTag<WaterSensor>("s1Tag"){};
        OutputTag<WaterSensor> s2Tag = new OutputTag<WaterSensor>("s2Tag"){};//TODO 不加{}创建的是OutputTag本类对象, 加上{}创建的是OutputTag的匿名子类对象
       // OutputTag<WaterSensor> s2Tag = new OutputTag<WaterSensor>("s2Tag", TypeInformation.of(WaterSensor.class));


        SingleOutputStreamOperator<WaterSensor> mainDS = wsDS.process(
                new ProcessFunction<WaterSensor, WaterSensor>() {
                    @Override
                    public void processElement(WaterSensor value, ProcessFunction<WaterSensor, WaterSensor>.Context ctx, Collector<WaterSensor> out) throws Exception {
                        String id = value.id;
                        if ("s1".equals(id)) {
                            //如果设备id是s1,将对应的数据放在侧输出流
                            ctx.output(s1Tag, value);
                        } else if ("s2".equals(id)) {
                            //如果设备id是s2,将对应的数据放在侧输出流
                            ctx.output(s2Tag, value);
                        } else {
                            //其他设备采集的信息，都放在主流中
                            out.collect(value);
                        }
                    }
                }
        );
        mainDS.print("主流：");
        SideOutputDataStream<WaterSensor> s1DS = mainDS.getSideOutput(s1Tag);
        SideOutputDataStream<WaterSensor> s2DS = mainDS.getSideOutput(s2Tag);
        s1DS.print("s1侧输出流：");
        s2DS.print("s2侧输出流：");


        env.execute();
    }
}
