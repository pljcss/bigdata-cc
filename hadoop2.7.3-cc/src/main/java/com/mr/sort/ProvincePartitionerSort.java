package com.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * 自定义分区
 * @author cs
 * @date 2020/11/17 12:59 下午
 */
public class ProvincePartitionerSort extends Partitioner<FlowBeanSort, Text> {

    @Override
    public int getPartition(FlowBeanSort flowBean, Text text, int numPartitions) {
        // key是手机号，value是流量信息

        // 获取手机号
        String prePhone = text.toString().substring(0, 3);

        int partition = 4;

        if ("136".equals(prePhone)) {
            partition = 0;
        } else if ("137".equals(prePhone)) {
            partition = 1;
        } else if ("138".equals(prePhone)) {
            partition = 2;
        } else if ("139".equals(prePhone)) {
            partition = 3;
        }


        return partition;
    }
}
