package com.mr.sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * 需要基于 flowsum 的输出进行处理
 *      13470253144	180	180	360（phone、upFlow、downFlow、sumFlow）
 *
 * @author cs
 * @date 2020/11/17 3:28 下午
 */
public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBeanSort, Text> {
    FlowBeanSort keyOutFlowBeanSort = new FlowBeanSort();
    Text valueOut = new Text();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");

        String phone = fields[0];
        long upFlow = Long.parseLong(fields[1]);
        long downFlow = Long.parseLong(fields[2]);
        long sumFlow = Long.parseLong(fields[3]);

        keyOutFlowBeanSort.setUpFlow(upFlow);
        keyOutFlowBeanSort.setDownFlow(downFlow);
        keyOutFlowBeanSort.setSumFlow(sumFlow);

        valueOut.set(phone);

        context.write(keyOutFlowBeanSort, valueOut);
    }
}
