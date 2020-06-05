package com.zhenglei.mapper;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

//3.关于四个泛型作用：
//    KEYIN(LongWritable类型):一行文本的开始位置，在整个文本的偏移量（用来确定当前是哪一行的，对于业务来说没有用）
//    VALUEIN(Text类型)：读到一行文本的内容，使用这个文本划分成单词
//    KEYOUT(Text类型):输出的单词
//    VALUEOUT(IntWritable类型):单词的统计次数
public class WordcountMapper extends Mapper<LongWritable, Text,Text, IntWritable> {

    //map阶段的业务逻辑，被maptask调用
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //将传入的数据按空格分割成单词
        String[] words = value.toString().split("\t");

//        //将单词输出为<单词，1>
//        for(String word:words){
//            //将单词作为key,将次数1作为value
//            //根据单词的分发，相同的key会进入相同的reduce task中
//            //context是mr框架提供的上下文
//            //还注意要使用Text，IntWritable类型
//            context.write(new Text(word),new IntWritable(1));
//        }
        //将第四个单词 进行统计
        String word = words[3];
        context.write(new Text(word),new IntWritable(1));
    }
}