package com.atguigu.flink.day09;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

public class Flink01_checkpoint {
    public static void main(String[] args) throws Exception {
        //TODO 1.指定流处理环境
        //Configuration conf = new Configuration();
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //启用检查点   interval：周期
        env.enableCheckpointing(5000L, CheckpointingMode.EXACTLY_ONCE);
        //检查点配置
        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //检查点存储位置  默认JM堆内存
        //checkpointConfig.setCheckpointStorage(new JobManagerCheckpointStorage());
        checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck");
        // 检查点模式（CheckpointingMode）  也可以在开启检查点的时候指定
        // checkpointConfig.setCheckpointingMode(CheckpointingMode.AT_LEAST_ONCE);
        //超时时间
        checkpointConfig.setCheckpointTimeout(60000L);
        //最小间隔时间  上一个检查点结束和下一个检查点起始时间间隔
        checkpointConfig.setMinPauseBetweenCheckpoints(2000L);//如果设置了最小时间间隔，那么最大并发检查点数据只能为1
        //最大并发检查点数量
        //checkpointConfig.setMaxConcurrentCheckpoints(1);
        //开启外部持久化存储  --job取消后，检查点是否保留
        checkpointConfig
                .setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
                //.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);
        //检查点连续失败次数
        checkpointConfig.setTolerableCheckpointFailureNumber(3);

        //设置操作hadoop的用户
        System.setProperty("HADOOP_USER_NAME","atguigu");

        //设置重启策略（和容错有关）
        //flink本身提供了容错机制，如果程序出现异常，会尝试进行恢复，通过重启恢复，默认重启Integer.MAX_VALUE次
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,3000L));

        // 设置状态后端
        // env.setStateBackend(new HashMapStateBackend());  ---状态存在TM堆内存
        // env.setStateBackend(new EmbeddedRocksDBStateBackend());  ---状态存在RocksDS

        //非对齐检查点
        //checkpointConfig.enableUnalignedCheckpoints();
        //对齐检查点超时时间
        //checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(30));

        // 通用增量(实验性的功能)
        //env.enableChangelogStateBackend(true);
        // 最终检查点(默认开启)
        //conf.set(ExecutionCheckpointingOptions.ENABLE_CHECKPOINTS_AFTER_TASKS_FINISH, false);//关闭  一般不会这么做




        //TODO 2.从指定的网络端口读取数据
        env.socketTextStream("hadoop102", 8888).name("haha").uid("source-id")

                //TODO 3. 将读取的数据进行转换   封装为二元组
                .flatMap(
                        (String s, Collector<Tuple2<String,Long>> collector) -> {//存在泛型擦除问题
                            String[] wordArr = s.split(" ");
                            for (String string : wordArr) {
                                collector.collect(Tuple2.of(string, 1L));
                            }

                        }
                ).uid("flatMap-id")//.returns(new TypeHint<Tuple2<String, Long>>() {})
                .returns(Types.TUPLE(Types.STRING,Types.LONG))

                //TODO 4.按照单词分组
                .keyBy(word -> word.f0)

                //TODO 5.聚合
                .sum(1).uid("sum-id")
                .print().uid("print-id");

        //TODO 执行
        env.execute();
    }
}
