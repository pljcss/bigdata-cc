package com.mr.join.reducejoin;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;

/**
 * @author cs
 * @date 2020/11/18 10:20 下午
 */
public class TableReducer extends Reducer<Text, TableBean, TableBean, NullWritable> {

    @Override
    protected void reduce(Text key, Iterable<TableBean> values, Context context)
            throws IOException, InterruptedException {

        // 存储所有订单集合
        ArrayList<TableBean> orderBeans = new ArrayList<>();

        // 存储产品信息
        TableBean pdBean = new TableBean();

        for (TableBean tableBean : values) {
            if ("order".equals(tableBean.getFlag())) {
                TableBean tmpBean = new TableBean();
                try {
                    BeanUtils.copyProperties(tmpBean, tableBean);
                    orderBeans.add(tmpBean);

                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            } else {
                try {
                    BeanUtils.copyProperties(pdBean, tableBean);
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                } catch (InvocationTargetException e) {
                    e.printStackTrace();
                }
            }
        }


        System.out.println(pdBean.getPname() + "===========================");
        for (TableBean tableBean : orderBeans) {
            tableBean.setPname(pdBean.getPname());
            context.write(tableBean, NullWritable.get());
        }




    }
}
