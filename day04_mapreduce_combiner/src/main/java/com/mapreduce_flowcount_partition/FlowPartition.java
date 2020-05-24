package com.mapreduce_flowcount_partition;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Description TODO
 * @Author Solarhzou
 * @Date 2020/5/23 20:02
 **/
public class FlowPartition extends Partitioner<Text, FlowBean> {
    @Override
    public int getPartition(Text text, FlowBean flowBean, int numPartitions) {
        if (text.toString().startsWith("135")) {
            return 0;
        } else if (text.toString().startsWith("136")) {
            return 1;
        } else if (text.toString().startsWith("137")) {
            return 2;
        } else if (text.toString().startsWith("138")) {
            return 3;
        }
        return 0;
    }
}
