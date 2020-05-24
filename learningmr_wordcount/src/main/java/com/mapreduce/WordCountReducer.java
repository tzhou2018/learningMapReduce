package com.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/20 9:45
 **/
/*
  Mapper 的泛型
    KEYIN: k2 的类型   Text 每个单词
    VALUEIN, v2 的类型 LongWritable  集合中泛型类型
    KEYOUT,  k3 的类型 Text         每个单词
    VALUEOUT v3 的类型 LongWritable 每个单词出现的次数
 */
public class WordCountReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    /*
    reduce 方法的作用是将K2 和V2转换为K3 和V3
    key:    K2
    values: 集合
    context:    MapReduce 的上下文对象
     */
    /*
    新   K2      V2
        hello   <1,1>
        world   <1,1,1>
        hadoop  <1,1,1>
        ---------------------
        K3      V3
        hello   2
        world   3
        hadoop  3
     */
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