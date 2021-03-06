package com.mr.nlineinputformat;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/16 5:51 下午
 */
public class NLineReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    LongWritable valueOut = new LongWritable();

    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context)
            throws IOException, InterruptedException {

        int sum = 0;
        for (LongWritable value : values) {
            sum += value.get();
        }

        valueOut.set(sum);
        context.write(key, valueOut);
    }
}
