package com.mr.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * CombineTextInputFormat
 * @author cs
 * @date 2020/11/10 5:15 下午
 */
public class WordCountCombineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        // 获取job对象
        Job job = Job.getInstance(conf);

        // 设置jar存储位置
        job.setJarByClass(WordCountCombineDriver.class);

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
        FileOutputFormat.setOutputPath(job, new Path("/Users/saicao/Desktop/tmp_del/mr/output/"));

        // 如果不设置InputFormat，则默认是 TextInputFormat.class
        job.setInputFormatClass(CombineTextInputFormat.class);
        // 虚拟存储切片最大值设置 4M
        CombineTextInputFormat.setMaxInputSplitSize(job, 4194304);
        // 设置20M（实际开发中可以设置成128M）
//        CombineTextInputFormat.setMaxInputSplitSize(job, 20971520);


        // 提交job
        boolean b = job.waitForCompletion(true);

        System.out.println(b);


    }
}
