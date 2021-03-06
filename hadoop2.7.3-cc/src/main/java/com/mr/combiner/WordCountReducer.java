package com.mr.combiner;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/17 11:45 下午
 */
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    IntWritable valueOut = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        valueOut.set(sum);

        context.write(key, valueOut);
    }

}
