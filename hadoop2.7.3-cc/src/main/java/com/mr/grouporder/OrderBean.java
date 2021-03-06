package com.mr.grouporder;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @author cs
 * @date 2020/11/18 12:21 下午
 */
public class OrderBean implements WritableComparable<OrderBean> {

    private int orderId;
    private double price;

    /**
     * 按 orderId 升序，按 price 降序
     * @param bean
     * @return
     */
    @Override
    public int compareTo(OrderBean bean) {
        if (bean.getOrderId() == this.orderId) {
            return bean.getPrice() > this.price ? 1 : -1;
        }
        return this.orderId > bean.getOrderId() ? 1 : -1;

    }

    @Override
    public void write(DataOutput out) throws IOException {
        out.writeInt(orderId);
        out.writeDouble(price);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        orderId = in.readInt();
        price = in.readDouble();
    }


    public OrderBean() {
    }

    public OrderBean(int orderId, double price) {
        this.orderId = orderId;
        this.price = price;
    }

    public int getOrderId() {
        return orderId;
    }

    public void setOrderId(int orderId) {
        this.orderId = orderId;
    }

    public double getPrice() {
        return price;
    }

    public void setPrice(double price) {
        this.price = price;
    }

    @Override
    public String toString() {
        return orderId + "\t" + price;
    }
}
