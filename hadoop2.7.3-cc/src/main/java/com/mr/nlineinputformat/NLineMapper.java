package com.mr.nlineinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/16 5:44 下午
 */
public class NLineMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    Text keyOut = new Text();
    LongWritable valueOut = new LongWritable(1);

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
