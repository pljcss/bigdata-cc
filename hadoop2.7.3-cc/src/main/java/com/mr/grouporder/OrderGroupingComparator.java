package com.mr.grouporder;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * @author cs
 * @date 2020/11/18 3:53 下午
 */
public class OrderGroupingComparator extends WritableComparator {

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        // 只要id相同，就认为是相同的key


        return super.compare(a, b);
    }
}
