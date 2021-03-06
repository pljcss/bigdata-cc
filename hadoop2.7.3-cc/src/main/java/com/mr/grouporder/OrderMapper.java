package com.mr.grouporder;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/18 2:57 下午
 */
public class OrderMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    OrderBean orderBean = new OrderBean();

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        // 切割
        String[] fields = line.split(" ");

        // 封装对象
        int orderId = Integer.parseInt(fields[0]);
        double price = Double.parseDouble(fields[2]);
        orderBean.setOrderId(orderId);
        orderBean.setPrice(price);

        context.write(orderBean, NullWritable.get());

    }
}
