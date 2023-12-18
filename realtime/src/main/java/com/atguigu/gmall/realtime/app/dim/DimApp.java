package com.atguigu.gmall.realtime.app.dim;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.atguigu.gmall.realtime.beans.TableProcess;
import com.atguigu.gmall.realtime.common.GmallConfig;
import com.atguigu.gmall.realtime.utils.HbaseUtil;
import com.atguigu.gmall.realtime.utils.MyKafkaUtil;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Connection;

import java.util.Properties;

public class DimApp {
    public static void main(String[] args) throws Exception {
        //TODO 1.基本环境准备
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        //TODO 2.检查点相关设置
        //2.1开启检查点
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //2.2 设置检查点超时时间
        env.getCheckpointConfig().setCheckpointTimeout(60000L);
        //2.3 设置job取消后检查点是否保留
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //2.4 设置两个检查点之间最小时间间隔
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(2000L);
        //2.5 设置重启策略
        env.setRestartStrategy(RestartStrategies.failureRateRestart(3, Time.days(30),Time.seconds(3)));
        //2.6 设置状态后端
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://hadoop102:8020/ck");
        //2.7 设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");
        //TODO 3.从kafka的主题中读取数据
        //3.1 声明消费的主题以及消费者组
        String topic = "topic_db";
        String groupId = "dim_app_group";
        //3.2 创建消费者对象
        KafkaSource<String> kafkaSource = MyKafkaUtil.getKafkaSource(topic, groupId);
        //3.3 消费数据  封装为流
        DataStreamSource<String> kafkaStrDS = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        //TODO 4.对流中数据进行类型转换并进行简单的ETL（过滤）   jsonStr -> jsonObj
        SingleOutputStreamOperator<JSONObject> jsonObjDS = kafkaStrDS.process(
                new ProcessFunction<String, JSONObject>() {

                    @Override
                    public void processElement(String jsonStr, ProcessFunction<String, JSONObject>.Context context, Collector<JSONObject> out) throws Exception {
                        //将jsonStr转换为jsonObj
                        try {
                            JSONObject jsonObj = JSON.parseObject(jsonStr);
                            String type = jsonObj.getString("type");
                            if (!"bootstrap-start".equals(type) && !"bootstrap-complete".equals(type)) {
                                out.collect(jsonObj);
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                    }
                }
        );
        //jsonObjDS.print(">>>>");

       //TODO 5.使用FlinkCDC从配置表中读取配置信息
        //5.1 创建gmall_confMySqlSource
        Properties props = new Properties();
        props.setProperty("useSSL", "false");
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("hadoop102")
                .port(3306)
                .databaseList("gmall_config") // set captured database
                .tableList("gmall_config.table_process_dim") // set captured table
                .username("root")
                .password("000000")
                .jdbcProperties(props)
                .serverTimeZone("Asia/Shanghai")
                .deserializer(new JsonDebeziumDeserializationSchema())
                .startupOptions(StartupOptions.initial())
                .build();
        //5.2 读取数据 封装为流
        DataStreamSource<String> mysqlStrDS = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mysqlSource");
       // mysqlStrDS.print(">>>");

         //TODO 6.将读取到每一条配置信息  封装为一个实体类对象
        SingleOutputStreamOperator<TableProcess> tableProcessDS = mysqlStrDS.map(
                new MapFunction<String, TableProcess>() {
                    @Override
                    public TableProcess map(String jsonStr) throws Exception {
                        //为了处理方便，将json字符串转换为json对象
                        JSONObject jsonObj = JSON.parseObject(jsonStr);
                        //获取操作类型
                        String op = jsonObj.getString("op");
                        TableProcess tableProcess = null;
                        if ("d".equals(op)) {
                            //说明从配置表中删除了一条数据  从before属性中获取删除前的配置表信息
                            tableProcess = jsonObj.getObject("before", TableProcess.class);
                        } else {
                            //c、r、u 都需要从after属性中获取配置表中的最新配置信息
                            tableProcess = jsonObj.getObject("after", TableProcess.class);
                        }
                        tableProcess.setOp(op);
                        return tableProcess;
                    }
                }
        );
        // tableProcessDS.print(">>>");

        //TODO 7.根据配置信息  提前在Hbase中建表或者删表
        tableProcessDS = tableProcessDS.map(
                new RichMapFunction<TableProcess,TableProcess>(){
                    private Connection hbaseConn;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        hbaseConn = HbaseUtil.getHbaseConnection();


                    }

                    @Override
                    public void close() throws Exception {
                        HbaseUtil.closeHbaseConnection(hbaseConn);
                    }

                    @Override
                    public TableProcess map(TableProcess tableProcess) throws Exception {
                        //获取对配置表进行的操作的类型
                        String op = tableProcess.getOp();
                        String sinkTable = tableProcess.getSinkTable();
                        String[] sinkFamilies = tableProcess.getSinkFamily().split(",");

                        if ("c".equals(op) || "r".equals(op)) {
                            //建表
                            HbaseUtil.createTable(hbaseConn, GmallConfig.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        } else if ("d".equals(op)) {
                            //删表
                            HbaseUtil.dropTable(hbaseConn, GmallConfig.HBASE_NAMESPACE, sinkTable);
                        } else {
                            //先删表
                            HbaseUtil.dropTable(hbaseConn, GmallConfig.HBASE_NAMESPACE, sinkTable);
                            //再建表
                            HbaseUtil.createTable(hbaseConn, GmallConfig.HBASE_NAMESPACE, sinkTable, sinkFamilies);
                        }
                        return tableProcess;
                    }
                }
        );

        //TODO 8.广播配置流---broadcast
        //TODO 9.将主流业务数据和广播流的配置数据进行关联---connect
        //TODO 10.对关联后的数据进行处理----process(过滤出维度数据)
        //TODO 11.将维度数据写到HBase对应的维度表中

        env.execute();
    }
}
