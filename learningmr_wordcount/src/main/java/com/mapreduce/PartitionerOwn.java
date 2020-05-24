package com.mapreduce;


import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/20 15:20
 **/
public class PartitionerOwn extends Partitioner<Text, LongWritable> {
    /*
    o:  表示K2
    o2： 表示V2
    i：  表示reduce个数
     */
    @Override
    public int getPartition(Text text, LongWritable longWritable, int i) {
        // 如果单词长度 >= 5， 进入第一个分区——>第一个reduceTask——>第一个reduce编号为0
        if (text.toString().length() >= 5) {
            return 0;
        } else {
            return 1;
        }
    }
}