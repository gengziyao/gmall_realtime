时间语义
    事件时间：当前数据产生的时间(从Flink1.12后，默认的时间语义)
    处理时间：在flink程序中对数据真正进行处理的时间
    摄入时间：进入source的时间(了解)
    在程序中到底使用哪种时间进行计算，就是所谓的时间语义
窗口分类以及具体的分配器
    窗口：将无限数据量分隔成一个个数据块进行处理
    时间窗口
        keyedDS.window()
        noKeyedDS.windowAll()
        滚动处理时间窗口-TumblingProcessingTimeWindows
        滑动处理时间窗口-SlidingProcessingTimeWindows
        处理时间会话窗口-ProcessingTimeSessionWindows

        滚动事件时间窗口-TumblingEventTimeWindows
        滑动事件时间窗口-SlidingEventTimeWindows
        事件时间会话窗口-EventTimeSessionWindows
    计数窗口
        底层实现Global Window
        keyedDS.countWindow()
        noKeyedDS.countWindowAll()

        滚动计数窗口
            .countWindow(窗口大小)
        滑动计数窗口
            .countWindow(窗口大小，滑动步长)
     全局窗口.window(GlobalWindows.create());
        计数窗口底层用的是全局窗口
开窗函数
    keyby
        .window
        .countWindow
    no-keyby(并行度为1)
        .windowAll
        .countWindowLL
窗口处理函数
    增量聚合函数
        reduce
            窗口中元素类型以及聚合后向下游传递类型必须一致
            如果窗口中只有一条元素，reduce方法不会执行，直接将这条数据传递到下游
            从第二条数据来到之后，每来一条计算一次，会保留计算结果，和新进窗口的元素进行归约
        aggregate
            窗口中元素类型、累加类型以及向下游传递的类型可以不一致
            需要重写4个方法
                createAccumulator:初始化累加器~~~属于当前窗口第一个元素进来的时候执行 一个窗口只会初始化一次
                add:当窗口中有数据来的时候，进行累加~~~只要窗口有元素进来就会进行累加
                getResult:获取累加的结果 ~~~ 当窗口触发计算的时候调用
                merge：会话窗口需要实现
    全量聚合函数
        apply
            需要创建WindowFunction实现，实现apply方法
        process
            需要创建ProcessWindowFunction实现，实现process方法
            process更底层，方法参数中是context上下文对象，可以获取窗口对象以及其他的一些信息
    增量 + 全量
        增量：来一条处理一条，存储中间计算结果，占用空间少
        全量：可以通过上下文对象获取更丰富的信息
        执行流程：窗口数据来一条交给增量函数处理一条，然后将最终处理的结果交给全量函数补充上下文获取到相关信息
窗口中设计的一些概念
    窗口分配器：指定开什么类型窗口
    处理函数：对窗口中的元素做什么样的操作
    窗口触发器：执行窗口触发计算的时机
    窗口移除器：在数据进入窗口前或者从窗口出去后做些移除操作(了解)
