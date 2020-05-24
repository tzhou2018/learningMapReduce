package com.mapreduceSort;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/20 21:50
 **/
public class SortReducer extends Reducer<PairWritable, Text, PairWritable, NullWritable> {
    // 自定义计数器：使用枚举
    public static enum MyCounter{
        REDUCE_INPUT_KEY_RECORDS,REDUCE_INPUT_VALUE_RECORDS
    }
    @Override
    protected void reduce(PairWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // 统计Reduce阶段key的个数
        context.getCounter(MyCounter.REDUCE_INPUT_KEY_RECORDS).increment(1L);
        for (Text value :
                values) {
            // 统计Reducer 阶段Value个个数
            context.getCounter(MyCounter.REDUCE_INPUT_VALUE_RECORDS).increment(1L);
            context.write(key, NullWritable.get());
        }
    }
}