package com.atguigu.flink.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import static org.apache.flink.table.api.Expressions.$;

public class FlinkSQL01_Base {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建表处理环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
/*  可以指定 流 批 处理模式
        EnvironmentSettings environmentSettings = EnvironmentSettings.newInstance()
                .inStreamingMode()
                .build();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env, environmentSettings);
*/
        //创建动态表
        tableEnv.executeSql("CREATE TABLE source ( \n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT \n" +
                ") WITH ( \n" +
                "    'connector' = 'datagen', \n" +
                "    'rows-per-second'='1', \n" +
                "    'fields.id.kind'='random', \n" +
                "    'fields.id.min'='1', \n" +
                "    'fields.id.max'='10', \n" +
                "    'fields.ts.kind'='sequence', \n" +
                "    'fields.ts.start'='1', \n" +
                "    'fields.ts.end'='1000000', \n" +
                "    'fields.vc.kind'='random', \n" +
                "    'fields.vc.min'='1', \n" +
                "    'fields.vc.max'='100'\n" +
                ");\n");

/*        TableResult tableResult = tableEnv.executeSql("select * from source");
        tableResult.print(); //job会自动提交*/
        Table sourceTable = tableEnv.sqlQuery("select * from source");
        //sourceTable.execute().print();
        //注册表对象，不注册会报错
//        tableEnv.createTemporaryView("source_table",sourceTable);
//        tableEnv.executeSql("select * from source_table").print();
        //也可以这样注册
       // tableEnv.executeSql("select * from " + sourceTable).print();

        // env.execute(); 如果最后操作的是API，必须要通过execute提交job,  如果最后操作的是 动态表，则不需要

        //通过tableAPI查询表中数据
        tableEnv
                .from("source")
                .where ($("id").isEqual(1))
                .select($("id"),$("ts"),$("vc")).execute().print();


        tableEnv.executeSql("CREATE TABLE sink (\n" +
                "    id INT, \n" +
                "    ts BIGINT, \n" +
                "    vc INT\n" +
                ") WITH (\n" +
                "'connector' = 'print'\n" +
                ");\n");

        //tableEnv.executeSql("insert into sink select * from source ");
        //tableEnv.sqlQuery("insert into sink select * from source ");
        tableEnv.sqlQuery("select * from source ").executeInsert("sink");
        tableEnv.sqlQuery("select id,vc from sink ");
    }
}
