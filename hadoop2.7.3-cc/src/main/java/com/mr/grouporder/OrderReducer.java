package com.mr.grouporder;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/18 3:18 下午
 */
public class OrderReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

    @Override
    protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context)
            throws IOException, InterruptedException {

        // 取 topN 就遍历几次
//        int counter=0;
//        for (NullWritable value : values) {
//            if (counter <= 2) {
//                context.write(key, NullWritable.get());
//            }
//
//            counter++;
//        }

        System.out.println("---------------");

        // 取 top1
        context.write(key, NullWritable.get());


    }
}
