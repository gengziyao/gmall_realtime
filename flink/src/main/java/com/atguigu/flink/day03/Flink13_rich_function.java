package com.atguigu.flink.day03;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;


/**
 * 该案例演示了富函数
 *      -在flink中，每一个算子的处理函数都存在一个对应富函数，命名 Rich+处理函数名
 *          例如MapFunction(接口)->RichMapFunction(抽象类)
 *      -富函数可以获取运行时上下文
 *      -富函数有生命周期
 *          open:  在每个并行任务启动的时候被调用，只会被调用一次,多个并行度调多次
 *          close: 在每个并行任务关闭的时候被调用，只会被调用一次,多个并行度调多次
 *          map:   每来一条元素都会被调用一次
 *     -如果是无界流，那么程序不会关闭，close方法不会被调用
 *
 */
public class Flink13_rich_function {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(2);
        DataStreamSource<Integer> dataDS = env.fromElements(1, 2, 3, 4);
        dataDS.map(
                new RichMapFunction<Integer, String>() {

                    @Override
                    public String map(Integer integer) throws Exception {
                        return  "数据是:" + integer;
                    }

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();
                        System.out.println("索引号是：" + runtimeContext.getIndexOfThisSubtask()
                                + ",名称是：" +
                                runtimeContext.getTaskNameWithSubtasks()
                         + "开启了");

                    }

                    @Override
                    public void close() throws Exception {
                        RuntimeContext runtimeContext = getRuntimeContext();
                        System.out.println("索引号是：" + runtimeContext.getIndexOfThisSubtask()
                                + ",名称是：" +
                                runtimeContext.getTaskNameWithSubtasks()
                                + "关闭了");

                    }
                }
        ).print();

        env.execute();
    }
}
