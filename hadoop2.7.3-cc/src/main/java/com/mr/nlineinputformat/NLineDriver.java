package com.mr.nlineinputformat;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.NLineInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * NLineInputFormat
 *      NLineInputFormat.setNumLinesPerSplit(job, 2);
 *      job.setInputFormatClass(NLineInputFormat.class );
 * @author cs
 * @date 2020/11/16 5:53 下午
 */
public class NLineDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置每个切片中划分两个2条记录
        NLineInputFormat.setNumLinesPerSplit(job, 2);
        job.setInputFormatClass(NLineInputFormat.class );

        job.setJarByClass(NLineDriver.class);
        job.setMapperClass(NLineMapper.class);
        job.setReducerClass(NLineReducer.class);

        // 设置Mapper阶段输出数据的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(LongWritable.class);

        // 设置最终数据输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(LongWritable.class);


        FileInputFormat.setInputPaths(job, new Path("/Users/saicao/Desktop/tmp_del/mr/input3/"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/saicao/Desktop/tmp_del/mr/output3/"));

        // 提交job
        boolean b = job.waitForCompletion(true);

        System.out.println(b);
    }
}
