package com.atguigu.flink.day03;

import org.apache.flink.api.common.functions.Partitioner;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 7种flink封装好的分区算子 + 1个自定义
 */

public class Flink14_partition {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(
                new Configuration()
        );

        env.setParallelism(4);

        DataStreamSource<String> stream = env.socketTextStream("hadoop102", 8888);

        // stream.shuffle().print();
        //stream.rebalance().print();
        //stream.rescale().print();
        //stream.broadcast().print();
        stream.global().print();

        //自定义
        stream.partitionCustom(
                new MyPartitioner(),value ->value
        ).print();

        env.execute();

    }
}

class MyPartitioner implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
        return Integer.parseInt(key) % numPartitions;
    }
}

