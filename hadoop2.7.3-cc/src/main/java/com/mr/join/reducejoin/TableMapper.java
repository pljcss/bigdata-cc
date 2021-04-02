package com.mr.join.reducejoin;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/18 10:03 下午
 */
public class TableMapper extends Mapper<LongWritable, Text, Text, TableBean> {

    String fileName;
    Text keyOut = new Text();
    TableBean tableBean = new TableBean();

    /**
     * Called once at the beginning of the task
     */
    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 获取文件名称
        FileSplit inputSplit = (FileSplit) context.getInputSplit();
        fileName = inputSplit.getPath().getName();
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();

        // 订单表
        if (fileName.startsWith("order")) {
            String[] fields = line.split("\t");

            // 封装key和value
            tableBean.setId(fields[0]);
            tableBean.setPid(fields[1]);
            tableBean.setAmount(Integer.parseInt(fields[2]));
            tableBean.setFlag("order");
            // 没有该值，设置为空字符串，否则序列化异常
            tableBean.setPname("");

            keyOut.set(fields[1]);

        } else { // 产品表
            String[] fields = line.split("\t");

            // 封装key和value
            tableBean.setId("");
            tableBean.setPid(fields[0]);
            tableBean.setAmount(0);
            tableBean.setFlag("pd");
            // 没有该值，设置为空字符串，否则序列化异常
            tableBean.setPname(fields[1]);

            keyOut.set(fields[0]);
        }

        System.out.println(keyOut + "::::" + tableBean + "################");
        // 写出
        context.write(keyOut, tableBean);

    }
}
