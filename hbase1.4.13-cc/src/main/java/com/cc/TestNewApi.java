package com.cc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;

/**
 * @Author: cs
 * @Date: 2021/3/25 3:34 下午
 * @Desc:
 *          connection.getAdmin() : 对应DDL
 *          connection.getTable() : 对应DML
 */
public class TestNewApi {

    public static void main(String[] args) throws IOException {
        Configuration config = HBaseConfiguration.create();
        config.set("hbase.zookeeper.quorum", "192.168.5.3");
        Connection connection = ConnectionFactory.createConnection(config);

        Admin admin = connection.getAdmin();

        boolean result = admin.tableExists(TableName.valueOf("sss"));

        System.out.println(result);

        admin.close();
    }
}
