package com.mr.compress;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @Author: cs
 * @Date: 2021/3/27 6:06 下午
 * @Desc:
 */
public class Test {
    private static volatile boolean flag = true;

    public static void main(String[] args) throws IOException {
        FileOutputStream fileOutputStream = new FileOutputStream("/Users/saicao/Desktop/word.txt",true);




        List<String> strings = Arrays.asList("hello", "world", "spark",
                "java", "spring", "hbase", "python", "he", "good", "test", "math");



        new Thread(new Runnable() {
            @Override
            public void run() {
                while (flag) {
                    File file = new File("/Users/saicao/Desktop/word.txt");
                    // m
                    long length = file.length()/1024/1024;
                    System.out.println(length);

                    if (length >= 10) {
                        flag = false;
                    }

                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();


        while (flag) {
            int i=0;
            StringBuffer sb = new StringBuffer();

            while (i < 1000) {
                int start = (int) (Math.random() * 5);
                int end = (int) (Math.random() * 5);
                if (start == 0) {
                    start+=1;
                }
                List<String> strings1 = strings.subList(start, end + start);

                for (int i1 = 0; i1 < strings1.size(); i1++) {
                    System.out.println(strings1);
                    sb.append(strings1.get(i1));
                    if (i1 != (strings1.size() - 1)) {
                        sb.append(" ");
                    } else {
                        sb.append("\n");
                    }

                }

                i++;
            }

        fileOutputStream.write(sb.toString().getBytes());

        }


    }
}
