package com.demo;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

/**
 * @author cs
 * @date 2020/11/1 11:35 下午
 */
public class ZookeeperDemo {
    private String connectString = "localhost:2181";
    private int sessionTimeout = 2000;
    private ZooKeeper zkClient;

    @Before
    public void test() throws IOException {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent event) {

            }
        });

        System.out.println(zkClient);


    }

    /**
     * 创建节点
     */
    @Test
    public void createNode() throws KeeperException, InterruptedException {
        String path = zkClient.create("/zkapitest",
                "helloworld".getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.PERSISTENT);

        System.out.println(path);
    }

    /**
     * 获取子节点并监听节点的变化
     */
    @Test
    public void getDataAndWatch() throws KeeperException, InterruptedException {
        List<String> children = zkClient.getChildren("/", true);
        for (String child : children) {
            System.out.println(child);
        }


    }
}
