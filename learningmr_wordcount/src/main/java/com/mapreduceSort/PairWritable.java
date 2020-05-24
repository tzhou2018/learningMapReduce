package com.mapreduceSort;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/20 21:51
 **/
public class PairWritable implements WritableComparable<PairWritable> {
    private String first;
    private int second;

    public String getFirst() {
        return first;
    }

    public void setFirst(String first) {
        this.first = first;
    }

    public int getSecond() {
        return second;
    }

    public void setSecond(int second) {
        this.second = second;
    }

    @Override
    public String toString() {
        return "PairWritable{" +
                "first='" + first + '\'' +
                ", second=" + second +
                '}';
    }

    // 实现排序规则
    @Override
    public int compareTo(PairWritable o) {
        // 先比较first， 如果first相同则比较second
        int result = this.first.compareTo(o.first);
        if (result == 0) {
            return this.second - o.second;
        }
        return result;
    }

    // 实现序列化
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(first);
        dataOutput.writeInt(second);
    }

    // 实现反序列化
    @Override
    public void readFields(DataInput dataInput) throws IOException {
        this.first = dataInput.readUTF();
        this.second = dataInput.readInt();
    }
}