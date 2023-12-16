package com.atguigu.hbase;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.CompareOperator;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.ColumnValueFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;


import java.io.IOException;
import java.util.List;

public class HbaseDemo {

    //获取连接
    public static Connection getHBaseConnection(){
        try {
            Configuration conf = new Configuration();
            conf.set("hbase.zookeeper.quorum","hadoop102,hadoop103,hadoop104");
            conf.set("hbase.zookeeper.property.clientPort","2181");

            //通过工厂方法获取连接
            Connection conn = ConnectionFactory.createConnection(conf);
            return conn;
        } catch (IOException e) {
            throw new RuntimeException();
        }
    }

    //关闭连接
    public static void closeHbaseConnection(Connection connection){

        if (connection != null && !connection.isClosed()){
            try {
                connection.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    //建表
    public static void createTable(Connection conn,String namespace,String tableName,String...families){

        if (families.length<1){
            System.out.println("列族必须指定");
            return;
        }

        try (Admin admin = conn.getAdmin()){//自动资源回收
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (admin.tableExists(tableNameObj)){
                System.out.println("要创建的表已存在");
                return;
            }

            TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(tableNameObj);

            for (String family : families) {
                ColumnFamilyDescriptor build = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes(family)).build();
                tableDescriptorBuilder.setColumnFamily(build);
            }

            admin.createTable(tableDescriptorBuilder.build());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //删表
    public static void dropTable(Connection conn,String namespace,String tableName){

        Admin admin = null;
        try {
            admin = conn.getAdmin();
            TableName tableNameObj = TableName.valueOf(namespace, tableName);
            if (!admin.tableExists(tableNameObj)){
                System.out.println("要删除的表不存在");
                return;
            }

            admin.disableTable(tableNameObj);
            admin.deleteTable(tableNameObj);
        } catch (IOException e) {
            e.printStackTrace();
        }finally {
            if (admin != null){
                try {
                    admin.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    //向表中添加一条数据
    public static void putCell(Connection conn,String namespace,String tableName,
                               String rowKey,String columnFamily,String columnName,String columnValue){

        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = conn.getTable(tableNameObj)){
            Put put = new Put(Bytes.toBytes(rowKey));
            put.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName),Bytes.toBytes(columnValue));
            table.put(put);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //从表中查询一条数据
    public static void getCell(Connection conn,String namespace,String tableName,String rowKey,
                               String columnFamily,String columnName){
        TableName tableNameObj = TableName.valueOf(namespace, tableName);
        try (Table table = conn.getTable(tableNameObj);){

            Get get = new Get(Bytes.toBytes(rowKey));
            get.readAllVersions();

            get.addColumn(Bytes.toBytes(columnFamily),Bytes.toBytes(columnName));
            Result result = table.get(get);
            //简单的用法 ,当前的最新版本
            //String cellValue = Bytes.toString(result.value());
            //System.out.println(cellValue);

            //具体版本数量，取决于建表的时候指定的列族的版本数
            Cell[] cells = result.rawCells();
            //等价于List<Cell> cells1 = result.listCells();

            for (Cell cell : cells) {
                System.out.println(Bytes.toString(result.getRow()) + ":" +
                        Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                        Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" +
                        Bytes.toString(CellUtil.cloneValue(cell)));
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    //从表中扫描数据scan
    public static void scanRows(Connection conn,String nameSpace, String tableName, String startRow, String stopRow){
        TableName tableName1 = TableName.valueOf(nameSpace, tableName);
        try(Table table = conn.getTable(tableName1)) {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRow));
            scan.withStopRow(Bytes.toBytes(stopRow),true);//true表示包括stopRow的值


            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    System.out.println(Bytes.toString(result.getRow())+ ":" +
                            Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" +
                            Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //带过滤扫描
    public static void scanRowsByFilter(Connection conn,String nameSpace, String tableName,
                                        String columnFamily,String column,String value,
                                        String startRow, String stopRow
    ){
        TableName tableName1 = TableName.valueOf(nameSpace, tableName);
        try(Table table = conn.getTable(tableName1)) {
            Scan scan = new Scan();
            scan.withStartRow(Bytes.toBytes(startRow));
            scan.withStopRow(Bytes.toBytes(stopRow),true);//true表示包括stopRow的值

            FilterList filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL);
            //列值过滤器，过滤出单列数据
            ColumnValueFilter columnValueFilter = new ColumnValueFilter(
                    Bytes.toBytes(columnFamily),
                    Bytes.toBytes(column),
                    CompareOperator.EQUAL,//
                    Bytes.toBytes(value)
            );
            //scan.setFilter(columnValueFilter);

            filterList.addFilter(columnValueFilter);

            // 过滤出符合添加的整行数据  结果包含其他列
            //注意：如果表中的一行数据没有查询的列，也会将这行数据查询出来
            SingleColumnValueFilter singleColumnValueFilter = new SingleColumnValueFilter(
                    // 列族
                    Bytes.toBytes(columnFamily),
                    // 列名
                    Bytes.toBytes(column),
                    // 匹配规则  一般为相等  也可以是大于等于 小于等于
                    CompareOperator.EQUAL,
                    Bytes.toBytes(value)
            );
            scan.setFilter(singleColumnValueFilter);
            filterList.addFilter(singleColumnValueFilter);

            //scan.setFilter(filterList);

            ResultScanner scanner = table.getScanner(scan);
            for (Result result : scanner) {
                List<Cell> cells = result.listCells();
                for (Cell cell : cells) {
                    System.out.println(Bytes.toString(result.getRow())+ ":" +
                            Bytes.toString(CellUtil.cloneFamily(cell)) + ":" +
                            Bytes.toString(CellUtil.cloneQualifier(cell)) + ":" +
                            Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //从表中删除数据
    public static void deleteColumn(Connection conn, String nameSpace, String tableName, String rowKey, String family, String column){
        TableName tableName1 = TableName.valueOf(nameSpace, tableName);
        try(Table table = conn.getTable(tableName1)) {
            Delete delete = new Delete(Bytes.toBytes(rowKey));
            //如果什么也不加，那么删除当前rowKey对应的列族的所有版本,会为每一个列族标记一个DeleteFamily

            //如果加上具体的列族和列，相当于delete命令，删除指定列的最新版本，标记为Delete
            delete.addColumn(Bytes.toBytes(family),Bytes.toBytes(column));

            //相当于deleteAll，删除指定列的所有版本，标记为DeleteColumn
            delete.addColumns(Bytes.toBytes(family),Bytes.toBytes(column));

            //相当于deleteAll指定列族,将指定列族下的所有列的版本都删除掉，标记为DeleteFamily
            delete.addFamily(Bytes.toBytes(family));

            table.delete(delete);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Connection conn = getHBaseConnection();

        //操作
        //建表
        //createTable(conn,"bigdata","test","info","msg");

        //删表
        //dropTable(conn,"bigdata","student2");

        //向表中put一条数据
        //putCell(conn,"bigdata","test","1001","info","name","zs");

        //从表中查询数据
        //getCell(conn,"bigdata","test","1001","info","name");

        //从表中扫描数据
        //scanRows(conn,"bigdata","student","1001","1002");

        //带过滤扫描
        scanRowsByFilter(conn,"bigdata","student","info","name123","ww","1001","1002");

        closeHbaseConnection(conn);
    }
}