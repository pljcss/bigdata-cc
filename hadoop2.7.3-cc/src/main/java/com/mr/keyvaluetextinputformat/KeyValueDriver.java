package com.mr.keyvaluetextinputformat;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueLineRecordReader;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * KeyValueTextInputFormat
 *      conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");
 *      job.setInputFormatClass(KeyValueTextInputFormat.class);
 *
 * @author cs
 * @date 2020/11/16 5:12 下午
 */
public class KeyValueDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 设置切割符
        conf.set(KeyValueLineRecordReader.KEY_VALUE_SEPERATOR, "\t");

        // 获取job对象
        Job job = Job.getInstance(conf);

        // 设置jar存储位置
        job.setJarByClass(KeyValueDriver.class);

        // 关联Map和Reduce类
        job.setMapperClass(KeyValueMapper.class);
        job.setReducerClass(KeyValueReducer.class);

        // 设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置 KeyValueTextInputFormat
        job.setInputFormatClass(KeyValueTextInputFormat.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/saicao/Desktop/tmp_del/mr/input2"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/saicao/Desktop/tmp_del/mr/output2"));

        // 提交job
        boolean b = job.waitForCompletion(true);
        System.out.println(b);
    }
}
