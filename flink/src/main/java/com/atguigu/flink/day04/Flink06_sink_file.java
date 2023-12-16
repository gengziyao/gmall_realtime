package com.atguigu.flink.day04;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputFileConfig;
import org.apache.flink.streaming.api.functions.sink.filesystem.bucketassigners.DateTimeBucketAssigner;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;
import java.time.ZoneId;

public class Flink06_sink_file {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启检查点
        env.enableCheckpointing(5000L);

        DataGeneratorSource<String> source = new DataGeneratorSource<String>(
                new GeneratorFunction<Long, String>() {
                    @Override
                    public String map(Long aLong) throws Exception {

                        return "数据：" + aLong;
                    }
                },
                Long.MAX_VALUE,
                RateLimiterStrategy.perSecond(1000),
                TypeInformation.of(String.class)
        );
        DataStreamSource<String> dataDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "data_source");

        final FileSink<String> sink = FileSink
                .forRowFormat(new Path("F:\\Atguigu\\第四阶段\\大数据_230710_2(尚硅谷大数据技术之Flink1.17)\\output"), new SimpleStringEncoder<String>("UTF-8"))
                .withOutputFileConfig(new OutputFileConfig("atguigu-",".log"))
                .withBucketAssigner(
                        new DateTimeBucketAssigner<>("yyyy-MM-dd HH", ZoneId.systemDefault())
                )
                .withRollingPolicy(
                        DefaultRollingPolicy.builder()
                                .withRolloverInterval(Duration.ofSeconds(10))
                                .withInactivityInterval(Duration.ofSeconds(5))
                                .withMaxPartSize(new MemorySize(1024))
                                .build())
                .build();

        dataDS.sinkTo(sink);

        env.execute();
    }
}
