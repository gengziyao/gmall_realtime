package com.atguigu.flink.day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Flink07_OpeState_ListState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env
                .socketTextStream("hadoop102", 8888)
                .map(new MyCountMapFunction())
                .print();

        env.execute();

    }


    private static class MyCountMapFunction implements MapFunction<String,String>, CheckpointedFunction {
        private int count=0;

        private ListState<Integer> countState;

        @Override
        public String map(String value) throws Exception {
            ++count;

            return "个数:" + count;
        }


        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            System.out.println("snapshotState...");
            //先清状态
            countState.clear();
            //将本地变量的值
            countState.add(count);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {

            System.out.println("~~~~~~~initializeState~~~~~~~");
            //当初始化方法执行的时候,判断是从快照中（检查点)恢复状态还是新建窗口

                ListStateDescriptor<Integer> stateDescriptor
                  = new ListStateDescriptor<Integer>("countState",Integer.class);
                countState = context.getOperatorStateStore().getListState(stateDescriptor);


            if (context.isRestored()) {
                Integer next = countState.get().iterator().next();
                count = next;
            }

            //  从 算子状态中 把数据 拷贝到 本地变量
/*            if (context.isRestored()) {
                for (Integer c : countState.get()) {
                    count += c;
                }
            }*/



        }
    }
}
