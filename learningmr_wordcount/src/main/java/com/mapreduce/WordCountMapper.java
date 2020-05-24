package com.mapreduce;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/20 9:45
 **/

/*
  Mapper 的泛型
    KEYIN: k1 的类型   LongWritable 行偏移量
    VALUEIN, v1 的类型 Text         一行的文本数据
    KEYOUT,  k2 的类型 Text         每个单词
    VALUEOUT v2 的类型 LongWritable 固定值1
 */
public class WordCountMapper extends Mapper<LongWritable, Text, Text, LongWritable> {
    /*
    map 方法是将k1 和v1 转换为k2 和v2
    key: 是k1
    value: 是v1
    context： 表示MapReduce 上下文对象
     */
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        /*
         K1         V1
         0        hello,world
         11       hello,hadoop
         ---------------------
         K2         V2
         hello      1
         world      1
         world      1
         hadoop     1
         */
        Text text = new Text();
        // 1. 对每一行的数据进行字符串拆分
//        String s = new String();
        String line = value.toString();
        String[] split = line.split(",");
        // 2. 遍历数组，获取每一个单词
        for (String str :
                split) {
            text.set(str);
            context.write(text,new LongWritable(1));
        }
    }
}