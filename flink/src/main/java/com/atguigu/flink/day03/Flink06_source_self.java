package com.atguigu.flink.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class Flink06_source_self {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration()
        );

        MyselfSource source = new MyselfSource();
        env.addSource(source).print();//1.12前
        //env.fromSource();1.12后
        env.execute();
    }
}


class MyselfSource implements SourceFunction<String>{
    @Override
    public void run(SourceContext<String> ctx) throws Exception {
        for (int i = 0;i <1000;i++){
            ctx.collect("数据"+ i);
            Thread.sleep(1000);
        }
    }

    @Override
    public void cancel() {

        //当应用被取消后，执行cancel方法
        System.out.println("~~~结束~~~");
    }
}