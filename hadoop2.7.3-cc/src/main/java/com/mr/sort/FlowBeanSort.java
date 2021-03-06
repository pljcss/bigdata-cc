package com.mr.sort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/17 3:04 下午
 */
public class FlowBeanSort implements WritableComparable<FlowBeanSort> {
    // 上行流量
    private long upFlow;
    // 下行流量
    private long downFlow;
    // 总流量
    private long sumFlow;

    public FlowBeanSort() {
    }

    public FlowBeanSort(long upFlow, long downFlow) {
        this.upFlow = upFlow;
        this.downFlow = downFlow;
        this.sumFlow = upFlow + downFlow;
    }

    /**
     * 比较
     * @param bean
     * @return
     */
    @Override
    public int compareTo(FlowBeanSort bean) {
        int result;
        if (this.sumFlow > bean.getSumFlow()) {
            result = -1;
        } else if (this.sumFlow < bean.getSumFlow()) {
            result = 1;
        } else {
            result = 0;
        }

        return result;
    }

    /**
     * 序列化方法
     * @param out
     * @throws IOException
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(upFlow);
        out.writeLong(downFlow);
        out.writeLong(sumFlow);
    }

    /**
     * 反序列化方法
     * @param in
     * @throws IOException
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        upFlow = in.readLong();
        downFlow = in.readLong();
        sumFlow = in.readLong();
    }

    public long getUpFlow() {
        return upFlow;
    }

    public void setUpFlow(long upFlow) {
        this.upFlow = upFlow;
    }

    public long getDownFlow() {
        return downFlow;
    }

    public void setDownFlow(long downFlow) {
        this.downFlow = downFlow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }

    @Override
    public String toString() {
        return upFlow + "\t" + downFlow + "\t" + sumFlow;
    }

}
