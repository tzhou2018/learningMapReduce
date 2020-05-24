package com.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/21 10:58
 **/
// 规约
public class MyCombiner extends Reducer<Text, LongWritable, Text, LongWritable> {
    @Override
    protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
        long count = 0;
        // 1. 遍历values 集合
        for (LongWritable value :
                values) {
            // 2. 将集合中的值相加
            count += value.get();
        }
        // 3. 将K3 和V3 写入上下文中
        context.write(key, new LongWritable(count));
    }
}