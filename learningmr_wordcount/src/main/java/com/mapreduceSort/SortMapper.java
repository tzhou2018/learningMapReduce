package com.mapreduceSort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/20 21:50
 **/
public class SortMapper extends Mapper<LongWritable, Text, PairWritable, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 自定义计数器
        Counter counter = context.getCounter("MR_COUNT", "MapReduceCounter");
        counter.increment(1L);
        // 1. 对每一行数据进行拆分，然后封装到 PairWritable 对象中，作为K2
        String[] split = value.toString().split("\t");
        PairWritable pairWritable = new PairWritable();
        pairWritable.setFirst(split[0]);
        pairWritable.setSecond(Integer.parseInt(split[1]));

        // 2. 将K2 和V2 写入上下文中
        context.write(pairWritable, value);
    }
}