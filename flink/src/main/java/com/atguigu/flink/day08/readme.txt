原始状态

托管状态
    键控状态
        作用范围：经过keyby分组后的每一组
        ValueState
            值状态：状态中存储的是一个值
            开发步骤：
                声明状态
                    ValueState<状态中存放数据类型> 变量名
                初始化状态
                    在open方法中
                    getRuntimeContext().getState(状态描述器ValueStateDescriptor(标记，存放数据类型))
                使用状态进行业务处理
                    .value()  从状态中获取存储值
                    .update() 将参数值更新到状态中
                    .clear()  清空状态
        ListState
        MapState
        ReducingState
        AggregatingState
        状态的TTL
            .enableTimeToLive(StateTtlConfig.newBuilder(Time.seconds(10)
            .setUpdateType(OnCreateAndWrite|OnReadAndWrite)
            .setStateVisibility
            .build())
    算子状态
        作用范围：每个算子并行子任务(独立分区)
        ListState(了解)
        UnionListState(了解)
            ---------implements CheckpointedFunction-----------
            snapshotState:对状态进行备份
            initializeState：初始化状态
        BroadcastState
            -广播
                ds.broadcast(广播状态描述器)
            -和主流进行关联
                connect
            -对关联后的数据进行处理
                process
            -[Keyed]BroadcastProcessFunction
                processElement:处理主流数据
                    从广播状态中读取数据
                    根据读取的数据进行业务判断
                processBroadcastElement: 处理广播流数据
                    将数据放到广播状态中
状态后端
    管理本地状态以及检查点的存储方式和位置
        Flink1.13前版本
                            状态                  检查点
        Memory            TM堆内存                JM堆内存
        Fs                TM堆内存                文件系统
        RockDB            RockDB库               文件系统

        Flink1.13以及以后版本
                                          状态                  检查点
        HashMapStateBackEnd            TM堆内存                JM堆内存|文件系统
        RockDB                         RockDB                 文件系统
    可以在如下地方配置状态后端
        程序
        提交命令
        flink-conf.xml