package com.atguigu.flink.day04;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.kafka.clients.producer.ProducerConfig;


/**
 * 该案例演示了sink算子--将数据写到kafka
 * 使用kafka连接器的时候一致性保证
 *      生产者: ack=-1,幂等性开启,事务
 *          KafkaSink底层实现了2pc(事务)
 *      消费者：手动维护偏移量
 *          KafkaSource-->KafkaSourceReader-->维护消费的偏移量
 * 如果我们使用Flink提供的KafkaSink向kafka中写数据，要想保证写入一致性，如要做如下配置
 *      开启检查点(底层是2pc，依赖检查点)
 *      .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
 *      .setTransactionalIdPrefix("区分流")
 *      设置事务的超时时间
 *          .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000 + "")
 *      在消费端，需要设置隔离级别
 *      .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
 */

public class Flink07_sink_kafka {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

       // env.enableCheckpointing(5000L);

        DataStreamSource<String> socketDS = env.socketTextStream("hadoop102", 8888);

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers("hadoop102:9092,hadoop103:9092,hadoop104:9092")
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic("first")
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                //.setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //.setTransactionalIdPrefix("atguigu") //设置事务id的前缀
                //.setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,15*60*1000+"") //kafka最大超时时间
                .build();

        socketDS.sinkTo(sink);
        
        env.execute();
    }
}
