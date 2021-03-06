package com.mr.myinputformat;

import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/16 6:58 下午
 *
 * Text ：路径 + 文件名
 * ByteWritable ：文件内容
 */
public class WholeFileInputFormat extends FileInputFormat<Text, ByteWritable> {
    @Override
    public RecordReader<Text, ByteWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException, InterruptedException {

        WholeRecordReader recordReader = new WholeRecordReader();

        recordReader.initialize(split, context);

        return recordReader;
    }
}
