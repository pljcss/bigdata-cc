package com.mr.etl;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/19 1:19 下午
 */
public class LogDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置加载jar包路径
        job.setJarByClass(LogDriver.class);

        // 关联 Map (没有了Reducer)
        job.setMapperClass(LogMapper.class);

        // 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,
                new Path("/Users/saicao/Desktop/tmp_del/mr/etl_log_input"));
        FileOutputFormat.setOutputPath(job,
                new Path("/Users/saicao/Desktop/tmp_del/mr/etl_log_output"));

        // 设置ReduceTask的数量为0
        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
    }
}
