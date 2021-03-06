package com.mr.compress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.io.compress.CompressionInputStream;
import org.apache.hadoop.io.compress.CompressionOutputStream;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/19 2:43 下午
 */
public class TestCompress {
    public static void main(String[] args) throws IOException, ClassNotFoundException {
        // 压缩
        // org.apache.hadoop.io.compress.BZip2Codec
        // org.apache.hadoop.io.compress.GzipCodec
//        compress("/Users/saicao/Desktop/tmp_del/mr/compress/web.txt",
//                "org.apache.hadoop.io.compress.DefaultCodec");

        // 解压缩
        decompress("/Users/saicao/Desktop/tmp_del/mr/compress/web.txt.bz2");



    }

    private static void decompress(String fileName) throws IOException {
        // 1、压缩方式检查
        CompressionCodecFactory codecFactory = new CompressionCodecFactory(new Configuration());
        CompressionCodec codec = codecFactory.getCodec(new Path(fileName));

        if (codec == null) {
            System.out.println("can not press");
            return;
        }

        // 2、获取输入流
        FileInputStream fis = new FileInputStream(new File(fileName));
        CompressionInputStream cis = codec.createInputStream(fis);

        // 3、获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + ".decode"));

        // 4、流的拷贝
        IOUtils.copyBytes(cis, fos, 1024 * 1024, false);

        // 5、关闭资源
        IOUtils.closeStream(fos);
        IOUtils.closeStream(cis);
        IOUtils.closeStream(fis);

    }

    private static void compress(String fileName, String compressType)
            throws IOException, ClassNotFoundException {

        // 1、获取输入流
        FileInputStream fis = new FileInputStream(fileName);

        // 2、加载
        Class<?> codecClass = Class.forName(compressType);
        CompressionCodec codec = (CompressionCodec) ReflectionUtils.newInstance(codecClass, new Configuration());

        // 2、获取输出流
        FileOutputStream fos = new FileOutputStream(new File(fileName + codec.getDefaultExtension()));
        // 获取具有压缩的输出流
        CompressionOutputStream cos = codec.createOutputStream(fos);

        // 3、流的拷贝
        IOUtils.copyBytes(fis, cos, 1024 * 1024, false);

        // 4、关闭资源
        IOUtils.closeStream(cos);
        IOUtils.closeStream(fos);
        IOUtils.closeStream(fis);

    }
}
