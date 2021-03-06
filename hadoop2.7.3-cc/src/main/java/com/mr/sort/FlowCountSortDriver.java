package com.mr.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/17 5:58 下午
 */
public class FlowCountSortDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 1、获取Job对象
        Job job = Job.getInstance(conf);

        // 2、设置jar路径
        job.setJarByClass(FlowCountSortDriver.class);

        // 3、关联 mapper和 reducer
        job.setMapperClass(FlowCountSortMapper.class);
        job.setReducerClass(FlowCountSortReducer.class);

        // 4、设置mapper输出的key和value类型
        job.setMapOutputKeyClass(FlowBeanSort.class);
        job.setMapOutputValueClass(Text.class);

        // 5、设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBeanSort.class);

        // 6、设置输入、输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/saicao/Desktop/tmp_del/mr/flow_sort_input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/saicao/Desktop/tmp_del/mr/flow_sort_output2"));


        // 设置分区
        job.setPartitionerClass(ProvincePartitionerSort.class);
        // 设置reduce个数
        job.setNumReduceTasks(5);

        // 7、提交job
        job.waitForCompletion(true);
    }
}
