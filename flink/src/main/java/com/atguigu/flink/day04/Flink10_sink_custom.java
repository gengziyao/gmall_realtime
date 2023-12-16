package com.atguigu.flink.day04;

import com.atguigu.flink.beans.WaterSensor;
import com.atguigu.flink.func.WaterSensorMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class Flink10_sink_custom {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        SingleOutputStreamOperator<WaterSensor> wsDS = env.socketTextStream("hadoop102", 8888)
                .map(new WaterSensorMapFunction());

        wsDS.addSink(
                new RichSinkFunction<WaterSensor>() {
                    private Connection conn;
                    @Override
                    public void open(Configuration parameters) throws Exception {

                        //建立连接
                         conn =
                                DriverManager.getConnection("jdbc:mysql://hadoop102:3306/test?serverTimezone=Asia/Shanghai&useUnicode=true&characterEncoding=UTF-8",
                                        "root", "000000");
                    }

                    @Override
                    public void close() throws Exception {
                        if (conn != null){
                            conn.close();
                        }
                    }

                    @Override
                    public void invoke(WaterSensor value, Context context) throws Exception {
                        //逻辑
                        //注册驱动
                        Class.forName("com.mysql.cj.jdbc.Driver");

                        //获取数据库操作对象
                        PreparedStatement ps = conn.prepareStatement("insert into test.ws values(?,?,?)");

                        //执行SQL语句
                        ps.setString(1,value.id);
                        ps.setLong(2,value.ts);
                        ps.setString(3,value.vc + "");
                        ps.execute();
                        //释放资源
                        ps.close();
                    }
                }
        );

        env.execute();
    }
}
