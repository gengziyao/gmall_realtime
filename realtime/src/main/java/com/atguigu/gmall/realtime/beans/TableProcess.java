package com.atguigu.gmall.realtime.beans;

import lombok.Data;

@Data
public class TableProcess {
    // 来源表
    String sourceTable;
    // 输出表
    String sinkTable;
    // 列族
    String sinkFamily;
    // 输出字段
    String sinkColumns;
    // rowkey
    String sinkRowKey;
    // 配置表操作: c r u d
    String op;
}