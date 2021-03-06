package com.mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * map阶段
 *
 * LongWritable 输入key
 * Text 输入value
 * Text 输出key
 * IntWritable 输出value
 *
 * @author cs
 * @date 2020/11/10 4:56 下午
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    Text text = new Text();
    IntWritable intWritable = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {

        // 获取一行
        String line = value.toString();
        // 切割单词
        String[] words = line.split(" ");

        // 循环写出
        for (String word : words) {
            // key
            text.set(word);
            context.write(text, intWritable);
        }

    }
}
