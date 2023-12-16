package com.atguigu.gmall.realtime.utils;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;

import java.io.IOException;

public class MyKafkaUtil {
    private static final String KAFKA_SERVER = "hadoop102:9092,hadoop103:9092,hadoop104:9092";
    public static KafkaSource<String> getKafkaSource(String topic, String groupId){

        KafkaSource<String> kafkaSource = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_SERVER)
                .setTopics(topic)
                .setGroupId(groupId)
                //在生产环境中，要向保证消费的精准一次，需要做如下设置，表示先从维护的偏移量开始消费，如果没找到，再从kafka最新偏移量消费
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.LATEST))
                //在学习的时候，我们直接从kafka的最新偏移量开始读取
                .setStartingOffsets(OffsetsInitializer.latest())
                //在生产环境中，要想保证生产端的精准一次，消费端不能消费预提交数据，需要设置隔离级别为读已提交
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
                //TODO 默认使用SimpleStringSchema，对kafka消息进行反序列化的时候，如果消息为空，那么处理不了，我们需要自定义反序列化器
                //.setValueOnlyDeserializer(new SimpleStringSchema())
                .setValueOnlyDeserializer(new DeserializationSchema<String>() {
                    @Override
                    public String deserialize(byte[] bytes) throws IOException {
                        if (bytes != null){
                            return new String(bytes);
                        }
                        return null;
                    }

                    @Override
                    public boolean isEndOfStream(String s) {
                        return false;
                    }

                    @Override
                    public TypeInformation<String> getProducedType() {
                        return TypeInformation.of(String.class);
                    }
                })
                .build();
        return kafkaSource;
    }
}
