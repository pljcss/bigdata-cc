package com.mr.flowsum;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/15 1:00 下午
 */
public class FlowCountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        // 1、获取Job对象
        Job job = Job.getInstance(conf);

        // 2、设置jar路径
        job.setJarByClass(FlowCountDriver.class);

        // 3、关联 mapper和 reducer
        job.setMapperClass(FlowCountMapper.class);
        job.setReducerClass(FlowCountReducer.class);

        // 4、设置mapper输出的key和value类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(FlowBean.class);

        // 5、设置最终输出的key和value类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(FlowBean.class);

        // 6、设置输入、输出路径
        FileInputFormat.setInputPaths(job, new Path("/Users/saicao/Desktop/tmp_del/mr/flow_input"));
        FileOutputFormat.setOutputPath(job, new Path("/Users/saicao/Desktop/tmp_del/mr/flow_output"));

        // 7、提交job
        job.waitForCompletion(true);

    }
}
