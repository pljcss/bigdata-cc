package com.mr.keyvaluetextinputformat;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * KeyValueTextInputFormat
 *
 * @author cs
 * @date 2020/11/16 5:10 下午
 */
public class KeyValueMapper extends Mapper<Text, Text, Text, IntWritable> {
    Text outKey = new Text();
    IntWritable outValue = new IntWritable(1);

    @Override
    protected void map(Text key, Text value, Context context)
            throws IOException, InterruptedException {

        System.out.println(key.toString() + "#############################");
        outKey.set(key);
        context.write(outKey, outValue);
    }
}
