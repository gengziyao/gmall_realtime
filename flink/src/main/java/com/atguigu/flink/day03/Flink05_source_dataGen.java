package com.atguigu.flink.day03;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


//数据生成器
public class Flink05_source_dataGen {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(3);

        DataGeneratorSource<String> source = new DataGeneratorSource<String>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {

                        return "数据：" + aLong;
                    }
                }, 100, RateLimiterStrategy.perSecond(2),
                TypeInformation.of(String.class)
        );
        env.fromSource(source, WatermarkStrategy.noWatermarks(),"data_source")
                .print();

        env.execute();


    }
}
