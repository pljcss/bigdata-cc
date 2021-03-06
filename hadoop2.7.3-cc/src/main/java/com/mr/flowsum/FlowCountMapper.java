package com.mr.flowsum;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/15 11:34 上午
 */
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

    Text keyOut = new Text();
    FlowBean valueOut = new FlowBean();

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 1、获取一行
        String line = value.toString();

        // 2、切割一行
        String[] fields = line.split("\t");
        String phone = fields[1];
        long upFlow = Long.parseLong(fields[fields.length - 1 - 2]);
        long downFlow = Long.parseLong(fields[fields.length - 1 - 1]);

        // 3、封装对象
        keyOut.set(phone);
        valueOut.setUpFlow(upFlow);
        valueOut.setDownFlow(downFlow);
        valueOut.setSumFlow(upFlow + downFlow);

        // 4、写出
        context.write(keyOut, valueOut);
    }
}
