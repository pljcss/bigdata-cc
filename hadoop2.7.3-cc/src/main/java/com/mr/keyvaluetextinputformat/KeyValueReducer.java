package com.mr.keyvaluetextinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/16 5:17 下午
 */
public class KeyValueReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    IntWritable outValue = new IntWritable();

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }

        outValue.set(sum);

        context.write(key, outValue);
    }
}
