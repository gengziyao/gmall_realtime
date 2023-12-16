package com.atguigu.flink.day09;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;

public class Flink02_kafka2pc_Sink {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("first")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                .setTransactionalIdPrefix("Flink02_kafka2pc_Sink") //设置事务id的前缀
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"") //kafka最大超时时间
                .build();

        socketDS.sinkTo(sink);

        env.execute();
    }
}
