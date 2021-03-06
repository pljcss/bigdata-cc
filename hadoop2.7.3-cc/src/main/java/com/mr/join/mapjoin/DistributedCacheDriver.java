package com.mr.join.mapjoin;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

/**
 * @author cs
 * @date 2020/11/19 10:15 上午
 */
public class DistributedCacheDriver {

    public static void main(String[] args)
            throws IOException, ClassNotFoundException, InterruptedException, URISyntaxException {
        // 获取job信息
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        // 设置加载jar包路径
        job.setJarByClass(DistributedCacheDriver.class);

        // 关联 Map (没有了Reducer)
        job.setMapperClass(DistributedCacheMapper.class);

        // 设置最终输出数据类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        FileInputFormat.setInputPaths(job,
                new Path("/Users/saicao/Desktop/tmp_del/mr/mapjoin_input"));
        FileOutputFormat.setOutputPath(job,
                new Path("/Users/saicao/Desktop/tmp_del/mr/mapjoin_output"));


        // 加载缓存数据 (URI是java.net包下的)
        job.addCacheFile(new URI("file:///Users/saicao/Desktop/tmp_del/mr/mapjoin_input_cache/pd.txt"));
        // Map端join的逻辑不需要reduce阶段，设置ReduceTask的数量为0
        job.setNumReduceTasks(0);

        job.waitForCompletion(true);
    }
}
