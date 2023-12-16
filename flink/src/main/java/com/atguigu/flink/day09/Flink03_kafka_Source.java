package com.atguigu.flink.day09;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.consumer.ConsumerConfig;

public class Flink03_kafka_Source {
    public static void main(String[] args) throws Exception {

        //TODO 1.指定流处理环境
        StreamExecutionEnvironment env
                = StreamExecutionEnvironment.getExecutionEnvironment();

        //TODO 2.从kafka主题中读取数据
        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setTopics("first")
                .setGroupId("testGroup")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")//TODO 读已提交 预提交的不消费
                // 从消费组提交的位点开始消费，不指定位点重置策略
                // .setStartingOffsets(OffsetsInitializer.committedOffsets())
                // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
                // .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                // 从时间戳大于等于指定时间戳（毫秒）的数据开始消费
                // .setStartingOffsets(OffsetsInitializer.timestamp(1657256176000L))
                // 从最早位点开始消费
                // .setStartingOffsets(OffsetsInitializer.earliest())
                // 从最末尾位点开始消费
                // .setStartingOffsets(OffsetsInitializer.latest());
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        DataStreamSource<String> kafkaStrDS
                = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "kafka_source");
        //TODO 3.打印
        kafkaStrDS.print();
        //TODO 4.提交
        env.execute();
    }
}
