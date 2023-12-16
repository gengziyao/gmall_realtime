水位线
    是一个逻辑时钟
    用来衡量事件时间进展的标记
    水位线的生成根据流中传递的元素的事件时间
    也是流中的一个元素，也会进行传递
    是时间戳毫秒
    单调递增
    一般用于【事件时间】窗口触发、关闭、定时器执行...

水位线的生成策略
    Flink提供了两种内置的生成策略，这两种策略都是周期性创建水位线
    水位线是流中的一个特殊的数据，也会进行传递
    单调递增底层
        forMonotonousTimestamps()
            ->new AscendingTimestampsWatermarks extends BoundedOutOfOrdernessWatermarks
    有界乱序底层
        forBoundedOutOfOrderness(Duration.ofMillis(2))
            ->new BoundedOutOfOrdernessWatermarks<>(maxOutOfOrderness)

            BoundedOutOfOrdernessWatermarks implements WatermarkGenerator{
                onEvent：流中每来一条数据都会被调用一次
                    比较最大事件时间，将最大时间放到maxTimestamp变量中存储起来
                onPeriodicEmit:周期性调用，默认200ms
                    生成watermark
                    output.emitWatermark(new Watermark(maxTimestamp - outOfOrdernessMillis - 1));
            }
    单调递增生成方式和有界乱序生成方式关系
        单调递增生成方式和有界乱序生成方式存在继承关系
        单调递增是有界乱序的子类
        可以这样理解：单调递增是有界乱序的特殊情况，乱序程度是0

以滚动事件时间窗口为例
    窗口的生命周期
        窗口对象什么时候创建
            当属于这个窗口第一个元素到来的时候，创建窗口对象
        窗口什么时候关闭
            水位线到了窗口最大时间 + 运行迟到时间
    窗口起始和结束时间如何确定
        start:向下取整
        end:start  + windowSize
        maxTimestamp: end - 1
    窗口什么时候被触发计算
        当窗口元素来到的时候，会用元素对应的watermark和窗口的最大时间进行比较，如果到了最大时间，会触发窗口的计算
        window.maxTimestamp() <= ctx.getCurrentWatermark()
    为什么滚动窗口是左闭右开

水位线传递
    上游1个并行度，下游n并行度：广播
    上游n个并行度，下游1个并行度：上游所有并行度中水位线最小值
    上游n个并行度，下游n个并行度：先广播再取最小

处理空闲数据源
    如果数据源中的某一个分区/分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator
    也不会获得任何新数据去生成 watermark。我们称这类数据源为空闲输入或空闲源。
    .withIdleness(Duration.ofSeconds(5))

迟到数据的处理
    水位线生成策略指定为有界乱序，指定乱序程度
        .<WaterSensor>forBoundedOutOfOrderness(Duration.ofMillis(2))
    窗口设置允许迟到时间
         .allowedLateness(Time.milliseconds(3))
    侧输出流

基于窗口实现join
    只支持内连接
    在处理事件时间窗口的时候，对于有界流，当程序执行结束后，会生成Long.MAX_VALUE作为水位线