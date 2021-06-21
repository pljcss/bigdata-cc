package com.cc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Iterator;

/**
 * @Author: cs
 * @Date: 2021/3/25 5:56 下午
 * @Desc:
 *         DML
 *          - 插入数据
 *          - 查询数据（get）
 *          - 查询数据（scan）
 *          - 删除数据
 *
 */
public class TestNewApi3 {
    private static Connection connection = null;
    static {
        try {
            // 获取配置信息
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "192.168.5.3");
            // 创建连接对象
            connection = ConnectionFactory.createConnection(config);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        putData("stu3", "rowkey1003", "info1","name", "rowkey1003");

//        getData("stu3", "rowkey1003");

        getScan("stu3", "");
    }

    /**
     * 插入一行数据
     * 注意：一次性添加多个列、一次性添加多个rowkey
     * @param tableName
     * @param rowkey
     * @param family
     * @param qualifier
     * @param value
     */
    public static void putData(String tableName, String rowkey, String family, String qualifier, String value) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 构造一行数据
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));

        try {
            if (table != null) {
                table.put(put);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        // 关闭资源
        try {
            if (table != null) {
                table.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 获取数据 get
     * @param tableName
     * @param rowkey
     */
    public static void getData(String tableName, String rowkey) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
        Get get = new Get(Bytes.toBytes(rowkey));

        try {
            if (table != null) {
                Result result = table.get(get);

                System.out.println(result);
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 获取数据 scan
     * @param tableName
     * @param rowkey
     */
    public static void getScan(String tableName, String rowkey) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        Scan scan = new Scan();


        try {

            // 扫描全表
            ResultScanner all = table.getScanner(scan);

            // 扫描指定列族
            ResultScanner info1 = table.getScanner(scan.addFamily(Bytes.toBytes("info1")));

            Iterator<Result> iterator = all.iterator();

            while (iterator.hasNext()) {
                Result next = iterator.next();
                System.out.println(next);
            }

            System.out.println(info1);
        } catch (IOException e) {
            e.printStackTrace();
        }


        try {
            table.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除数据
     * @param tableName
     */
    public static void delete(String tableName) {
        Table table = null;
        try {
            table = connection.getTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

//        Delete delete = new Delete();
//        table.delete();
    }


    /**
     * 关闭资源
     */
    public static void close() {
        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
