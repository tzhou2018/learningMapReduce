package com.mapreducer_sort;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @Description TODO
 * @Author Solarhzou
 * @Date 2020/5/23 17:02
 **/
public class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        FlowBean flowBean = new FlowBean();
        String[] split = value.toString().split("\t");
        // 获取手机号，作为V2
        String phoneNum = split[0];
        // 获取其他流量字段，封装flowBean,作为K2
        flowBean.setUpFlow(Integer.parseInt(split[1]));
        flowBean.setDownFlow(Integer.parseInt(split[2]));
        flowBean.setUpCountFlow(Integer.parseInt(split[3]));
        flowBean.setDownCountFlow(Integer.parseInt(split[4]));
        // 将K2 和V2写入上下文对象
        context.write(flowBean, new Text(phoneNum));
    }
}
