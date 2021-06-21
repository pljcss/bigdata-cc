package com.demo;

import java.util.*;

/**
 * @Author: cs
 * @Date: 2021/4/10 10:56 下午
 * @Desc:
 */
public class JavaTest {
    public static void main(String[] args) throws InterruptedException {

        while (true) {
            Thread.currentThread().setName("JavaTestttt");
            System.out.println(Thread.currentThread().getId());
            System.out.println("hello");
            Thread.sleep(500);
        }
        
    }
}
