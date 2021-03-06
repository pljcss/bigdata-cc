package com.mr.join.mapjoin;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.HashMap;

/**
 * @author cs
 * @date 2020/11/19 10:23 上午
 */
public class DistributedCacheMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    HashMap<String, String> pdMap = new HashMap<>(10);
    Text k = new Text();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        // 缓存小表
        URI[] cacheFiles = context.getCacheFiles();
        String path = cacheFiles[0].getPath().toString();
        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));

        String line;
        while (StringUtils.isNotEmpty(line = reader.readLine())) {
            // 切割 [01	小米]
            String[] fields = line.split("\t");
            pdMap.put(fields[0], fields[1]);
        }

        // 关闭资源
        IOUtils.closeStream(reader);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

        System.out.println(pdMap);
        // 获取一行
        String line = value.toString();
        String[] fields = line.split("\t");

        // 获取pid [1001	01	1]
        String pid = fields[1];
        // 获取pName
        String pName = pdMap.get(pid);

        // 拼接
        String newLine = line + "\t" + pName;

        // 写出
        k.set(newLine);
        context.write(k, NullWritable.get());
    }
}
