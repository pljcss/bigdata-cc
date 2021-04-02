package com.mr.wordcount;

import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/10 5:15 下午
 *
 * 读取输入文件格式是lzo的压缩文件
 */
public class WordCountDriverClusterLzo {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        // 获取job对象
        Job job = Job.getInstance(conf);

        // 设置jar存储位置
        job.setJarByClass(WordCountDriverClusterLzo.class);

        // 关联Map和Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置 InputFormat
//        Class<?> aClass = Class.forName("com.hadoop.mapreduce.LzoTextInputFormat");
        job.setInputFormatClass(LzoTextInputFormat.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));


        // 提交job
        job.waitForCompletion(true);

    }
}
