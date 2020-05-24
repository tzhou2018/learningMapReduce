package com.mapreduce_reduce_join;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @Descripion TODO
 * @Author Solarzhou
 * @Date 2020/5/21 22:00
 **/
public class JobMain extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        // 启动一个任务
        Configuration configuration = new Configuration();
        int run = ToolRunner.run(configuration, new JobMain(), args);
        System.exit(run);
    }

    @Override
    public int run(String[] args) throws Exception {
        // 创建任务对象
        Job job = Job.getInstance(super.getConf(), "mapreduce_join");
        // 打包时放在集群运行时，需要做一个配置
        job.setJarByClass(JobMain.class);
        // 1. 设置读取文件的类： K1 V1
        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job, new Path("hdfs://localhost:9000/user/hadoop/input/reduce_join"));
        // 2. 设置Mapper类
        job.setMapperClass(ReduceJoinMapper.class);
        // 设置Map 阶段的输出类型：K2 和V2 的类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // TODO shuffle 阶段，采用默认分区，排序，规约，分组
        // TODO 3.4.5.6. 采用默认方式
        // 设置我们的分区类
//        job.setPartitionerClass(FlowPartition.class);
        // 设置规约类
//        job.setCombinerClass(MyCombiner.class);
        // 7. 设置Reducer类
        job.setReducerClass(ReduceJoinReducer.class);
        // 设置Reducer阶段输出类型
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setMapOutputValueClass(LongWritable.class);
        // 设置Reduce 的个数
//        job.setNumReduceTasks(4);
        // 8. 设置输出类
        job.setOutputFormatClass(TextOutputFormat.class);
        // 设置输出的路径
        TextOutputFormat.setOutputPath(job, new Path("hdfs://localhost:9000/user/hadoop/out/reduce_join_out"));
        boolean b = job.waitForCompletion(true);
        return b ? 0 : 1;
    }
}