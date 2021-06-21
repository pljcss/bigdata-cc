package com.cc.utf;

import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * @Author: cs
 * @Date: 2021/4/1 6:23 下午
 * @Desc:
 *          自定义UDF
 */
public class MyUdf extends UDF {

    /**
     * 自定义UDF需要提供evaluate函数
     */
    public String evaluate(String str){

        return String.valueOf(str.length());
    }
}
