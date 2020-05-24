package com.mapreduce_flowcount;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/21 22:00
 **/
public class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // 1. 拆分手机号
        String[] split = value.toString().split("\t");
        String phoneNum = split[0];
        // 2. 获取四个流量字段
        FlowBean flowBean = new FlowBean();
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));
        // 3. 将K2 和V2 写入上下文对象
        context.write(new Text(phoneNum), flowBean);
    }
}