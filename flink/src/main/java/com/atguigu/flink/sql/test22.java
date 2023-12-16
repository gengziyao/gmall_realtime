package com.atguigu.flink.sql;

import com.atguigu.flink.beans.WaterSensor;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.util.ratelimit.RateLimiterStrategy;
import org.apache.flink.connector.datagen.source.DataGeneratorSource;
import org.apache.flink.connector.datagen.source.GeneratorFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class test22 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //准备流
/*        DataStreamSource<WaterSensor> sourceDS = env.fromElements(
                new WaterSensor("s1", 1L, 1),
                new WaterSensor("s1", 2L, 2),
                new WaterSensor("s2", 2L, 2),
                new WaterSensor("s3", 3L, 3),
                new WaterSensor("s4", 4L, 4)
        );*/

        DataGeneratorSource<WaterSensor> source = new DataGeneratorSource<WaterSensor>(

                new GeneratorFunction<Long, WaterSensor>() {
                    @Override
                    public WaterSensor map(Long aLong) throws Exception {
                        return new WaterSensor(aLong+"",aLong,(int)(aLong%100));
                    }
                }, 100, RateLimiterStrategy.perSecond(2),
                TypeInformation.of(WaterSensor.class)
        );
        DataStreamSource<WaterSensor> sourceDS = env.fromSource(source, WatermarkStrategy.noWatermarks(), "data_source");

        Table sensorTable = tableEnv.fromDataStream(sourceDS);

        tableEnv.createTemporaryView("sensor",sensorTable);
        //tableEnv.executeSql("select * from sensor").print();

        Table filterTable = tableEnv.sqlQuery("select id,ts,vc from sensor where ts>2 ");
        Table sumTable = tableEnv.sqlQuery("select id,sum(vc) from sensor group by id ");

        //DataStream<Row> filterDS = tableEnv.toDataStream(filterTable);
        DataStream<WaterSensor> filterDS = tableEnv.toDataStream(filterTable, WaterSensor.class);

        DataStream<Row> sumDS = tableEnv.toChangelogStream(sumTable);

        // TODO 通过打印结果可以发现 流会额外开一个线程来打印结果，所以 追加 和 回撤 都会打印; 但是动态表不会，如果上面一个表一直打印，则下面的表将没有机会打印
        filterDS.print("追加");
        sumDS.print("回撤");

        env.execute();

    }
}
