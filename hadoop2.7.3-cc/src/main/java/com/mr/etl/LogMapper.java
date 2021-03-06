package com.mr.etl;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/19 1:08 下午
 */
public class LogMapper extends Mapper<LongWritable, Text, Text, NullWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 获取一行
        String line = value.toString();
        // 解析数据
        boolean result = parseLog(line, context);

        if (!result) {
            return;
        }

        context.write(value, NullWritable.get());
    }

    /**
     * 处理log
     * @param line
     * @param context
     * @return
     */
    private boolean parseLog(String line, Context context) {
        String[] fields = line.split(" ");

        if (fields.length > 11) {
            // 设置计数器（可以在控制台打印的输出信息中查看）
            context.getCounter("map", "true").increment(1);
            return true;
        } else {
            // 设置计数器（可以在控制台打印的输出信息中查看）
            context.getCounter("map", "false").increment(1);
            return false;
        }

    }
}
