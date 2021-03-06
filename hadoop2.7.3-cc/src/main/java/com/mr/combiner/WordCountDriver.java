package com.mr.combiner;

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
 * @date 2020/11/17 11:50 下午
 */
public class WordCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        // 获取job对象
        Job job = Job.getInstance(conf);

        // 设置jar存储位置
        job.setJarByClass(WordCountDriver.class);

        // 关联Map和Reduce类
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // 设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 设置输入路径和输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/saicao/Desktop/tmp_del/mr/input/"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/saicao/Desktop/tmp_del/mr/combiner_output/"));

        // 设置 Combiner
        job.setCombinerClass(WordCountCombiner.class);

        // 提交job
        boolean b = job.waitForCompletion(true);

        System.out.println(b);

    }
}
