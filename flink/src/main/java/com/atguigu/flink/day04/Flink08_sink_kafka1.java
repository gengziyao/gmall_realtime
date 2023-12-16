package com.atguigu.flink.day04;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class Flink08_sink_kafka1 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        KafkaSink<WaterSensor> sink = KafkaSink.<WaterSensor>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(
                        new KafkaRecordSerializationSchema<WaterSensor>() {
                            @Nullable
                            @Override
                            public ProducerRecord<byte[], byte[]> serialize(WaterSensor ws, KafkaSinkContext context, Long timestamp) {
                                return new ProducerRecord<byte[], byte[]>("first",ws.id.getBytes(),ws.toString().getBytes());
                            }
                        }
                )
                .build();

        wsDS.sinkTo(sink);

        env.execute();

    }
}
