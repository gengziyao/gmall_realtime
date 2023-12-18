package com.atguigu.gmall.realtime.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HbaseUtil {
    public static Connection getHbaseConnection(){
        try {
            Configuration conf = new Configuration();
            // 配置 Zookeeper
            conf.set("hbase.zookeeper.quorum", "hadoop102,hadoop103,hadoop104");
            return ConnectionFactory.createConnection(conf);
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    public static void closeHbaseConnection(Connection conn){
        if (conn !=null && !conn.isClosed()){
            try {
                conn.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    //删表
    public static void dropTable(Connection conn, String namespace, String tableName) {
        try (Admin admin = conn.getAdmin()) {
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            //判断要删除的表是否存在
            if (!admin.tableExists(tableNameObj)) {
                System.out.println("要删除的" + namespace + "下的" + tableName + "不存在");
                return ;
            }
            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //建表
    public static void createTable(Connection conn,String namespace,String tableName,String...families){
        if(families.length < 1){
            System.out.println("创建表必须指定至少一个列族");
            return ;
        }
        try (Admin admin = conn.getAdmin()){
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            //判断要创建的表是否存在
            if(admin.tableExists(tableNameObj)){
                System.out.println("要创建的" + namespace + "下的" + tableName + "已存在");
                return;
            }
            System.out.println("创建的" + namespace + "下的" + tableName + "表");
            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);
            for (String family : families) {
                ColumnFamilyDescriptor familyDescriptor
                        = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(familyDescriptor);
            }
            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }


}
}
