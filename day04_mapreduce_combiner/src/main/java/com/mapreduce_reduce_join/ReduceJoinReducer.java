package com.mapreduce_reduce_join;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @Description TODO
 * @Author Solarhzou
 * @Date 2020/5/24 10:12
 **/
public class ReduceJoinReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        String first = "";
        String second = "";
        for (Text value : values) {
            if (value.toString().startsWith("p")) {
                first = value.toString();
            } else {
                second = value.toString();
            }
        }
        if (first.equals("")) {
            context.write(key, new Text("Null" + "\t" + second));
        } else {
            context.write(key, new Text(first + "\t" + second));
        }
    }
}
