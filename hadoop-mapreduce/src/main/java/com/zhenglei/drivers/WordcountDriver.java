package com.zhenglei.drivers;

import com.zhenglei.mapper.WordcountMapper;
import com.zhenglei.reducer.WordcountReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class WordcountDriver {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration configuration = new Configuration();
        Job job = Job.getInstance(configuration);

        //指定本程序jar包所在的路径
        job.setJarByClass(WordcountDriver.class);

        //利用反射指定job要使用的mapper业务类
        job.setMapperClass(WordcountMapper.class);
        job.setReducerClass(WordcountReducer.class);

        //利用反射，指定mapper输出数据的kv类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //设置最终(也就是reduce)输出的数据类型
        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        //指定job输入原始文件所在目录
        FileInputFormat.setInputPaths(job, new Path("hdfs://192.168.59.10:8020/test/bigtable"));
        //指定job的输出结果
        FileOutputFormat.setOutputPath(job, new Path("hdfs://192.168.59.10:8020/test/result"));

        //将job中配置的相关参数，提交给yarn运行
        //等待集群完成工作
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }
}