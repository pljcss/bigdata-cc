package com.cc;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

/**
 * @Author: cs
 * @Date: 2021/3/25 3:21 下午
 * @Desc:
 *          DDL
 *              1、判断表是否存在
 *              2、创建表
 *              3、创建命名空间
 *              4、删除表
 *          DML
 *              插入数据
 *              查询数据（get）
 *              查询数据（scan）
 *              删除数据
 */
public class TestOldApi {

    public static void main(String[] args) throws IOException {
        System.out.println(isTableExist("aaa"));
    }
    /**
     * 判断表是否存在
     */
    public static boolean isTableExist(String tableName) throws IOException {
        HBaseConfiguration config = new HBaseConfiguration();
        config.set("hbase.zookeeper.quorum", "192.168.5.3");
        HBaseAdmin admin = new HBaseAdmin(config);
        boolean result = admin.tableExists(tableName);

        admin.close();

        return result;
    }


}
