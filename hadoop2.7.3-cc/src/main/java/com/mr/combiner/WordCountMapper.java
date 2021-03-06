package com.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/17 11:41 下午
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Text keyOut = new Text();
    IntWritable valueOut = new IntWritable(1);

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] words = line.split(" ");

        for (String word : words) {
            keyOut.set(word);
            context.write(keyOut, valueOut);
        }

    }
}
