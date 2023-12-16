package com.atguigu.flink.day04;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink09_sink_jdbc {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        wsDS.addSink(
                JdbcSink.<WaterSensor>sink(
                        "insert into test.ws values(?,?,?)",
                        new JdbcStatementBuilder<WaterSensor>() {
                            @Override
                            public void accept(PreparedStatement preparedStatement, WaterSensor waterSensor) throws SQLException {
                                preparedStatement.setString(1,waterSensor.id);
                                preparedStatement.setLong(2,waterSensor.ts);
                                preparedStatement.setString(3,waterSensor.vc+"");
                            }
                        },

                        JdbcExecutionOptions.builder()
                                .withBatchIntervalMs(5000L)
                                .withBatchSize(5)
                                .withMaxRetries(3)
                                .build(),

                        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                                .withUrl("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8")
                                .withUsername("root")
                                .withPassword("000000")
                                .withConnectionCheckTimeoutSeconds(60) // 重试的超时时间
                                .build()

                )
        );

        env.execute();
    }
}
