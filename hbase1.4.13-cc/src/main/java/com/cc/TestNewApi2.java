package com.cc;

import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;

import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @Author: cs
 * @Date: 2021/3/25 3:46 下午
 * @Desc:
 *      DDL操作
 *          - 判断表是否存在
 *          - 创建表
 *          - 创建命名空间
 *          - 删除表
 *
 */
public class TestNewApi2 {

    private static Connection connection = null;
    private static Admin admin = null;

    static {
        try {
            // 获取配置信息
            Configuration config = HBaseConfiguration.create();
            config.set("hbase.zookeeper.quorum", "192.168.5.3");
            // 创建连接对象
            connection = ConnectionFactory.createConnection(config);
            // 创建admin对象
            admin = connection.getAdmin();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
//        createTable("table-api-test","info1","info2");
        System.out.println(isExistTable("table-api-test"));
//        deleteTable("table-api-test");

        createNameSpace("namespace-api");
    }

    /**
     * 建表
     * @param tableName
     * @param columnFamilies
     */
    public static void createTable(String tableName, String... columnFamilies) {

        // 创建表描述器物
        HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(tableName));

        // 循环添加列族信息
        for (String columnFamily : columnFamilies) {
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(columnFamily);
            // 设置列族版本
            hColumnDescriptor.setMaxVersions(2);
            tableDescriptor.addFamily(hColumnDescriptor);
        }

        try {
            // 创建表
            admin.createTable(tableDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断表是否存在
     * @param tableName
     * @return
     */
    public static boolean isExistTable(String tableName) {
        boolean tableExists = false;
        try {
            tableExists = admin.tableExists(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }

        return tableExists;
    }

    /**
     * 删除表
     * @param tableName
     */
    public static void deleteTable(String tableName) {
        try {
            // disable
            admin.disableTable(TableName.valueOf(tableName));
            // delete
            admin.deleteTable(TableName.valueOf(tableName));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 创建命名空间（）
     * TODO 没成功，后续继续DeBug
     *
     * @param nameSpace
     */
    public static void createNameSpace(String nameSpace) {
        NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(nameSpace).build();

        System.out.println(namespaceDescriptor);
        System.out.println(nameSpace);

        try {
            admin.createNamespace(namespaceDescriptor);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 关闭资源
     */
    public static void close() {
        if (admin != null) {
            try {
                admin.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (connection != null) {
            try {
                connection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
