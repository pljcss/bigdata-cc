package com.mr.sort;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/17 3:50 下午
 */
public class FlowCountSortReducer extends Reducer<FlowBeanSort, Text, Text, FlowBeanSort> {
    @Override
    protected void reduce(FlowBeanSort key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {

        for (Text value : values) {
            context.write(value, key);
        }
    }
}
