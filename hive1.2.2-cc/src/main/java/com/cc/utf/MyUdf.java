package com.cc.utf;

import org.apache.hadoop.hive.ql.exec.UDF;

/**
 * @Author: cs
 * @Date: 2021/4/1 6:23 下午
 * @Desc:
 */
public class MyUdf extends UDF {


    /**
     * 自定义UDF需要提供evaluate函数
     */
    public String evaluate(String ac){

        return "";
    }
}
