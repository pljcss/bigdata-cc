package com.mr.flowsum;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/15 12:08 下午
 */
public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {

    FlowBean flowBean = new FlowBean();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context)
            throws IOException, InterruptedException {

        // 1、累加求和
        long upFlowSum = 0;
        long downFlowSum = 0;
        long sumFlowSum = 0;
        for (FlowBean flowBean : values) {
            upFlowSum += flowBean.getUpFlow();
            downFlowSum += flowBean.getDownFlow();
            sumFlowSum += flowBean.getSumFlow();
        }

        flowBean.setUpFlow(upFlowSum);
        flowBean.setDownFlow(downFlowSum);
        flowBean.setSumFlow(sumFlowSum);

        context.write(key, flowBean);

    }
}
