分流算子
    filter
        缺点：要分出几条流出来，就需要将源流数据复制几份进行过滤，效率低
    侧输出流
        定义侧输出流标签
            OutputTag 标签名 = new OutputTag<侧流中数据类型>(){}
            OutputTag 标签名 = new OutputTag<侧流中数据类型>(侧流标记id,TypeInfomation类型)
            注意：在创建侧流标签对象的时候，泛型会被擦除
        将不同的数据放到侧流中
            ctx.output(侧流标签对象,要放的数据);
        取出侧输出流数据
            主流.getSideOutput(侧流标签对象)

合流算子
    union
        将两条或者多条流进行合并
            ds1.union(ds2).union(ds3)
            ds1.union(ds2,ds3)
        要求：参与合并的流的数据类型必须要一致
    connect
        将两条流的数据合并在一起
        参与合并的流的数据类型可以不一致
            ConnectedStreams 名  = ds1.connect(ds2);
        对连接后的数据进行处理：map faltmap process
        但是因为参与连接的流的数据类型不一致，在处理的时候需要分别对两条流数据数据进行处理(需要实现两个方法分别处理流中数据)
