package com.demo.mr.map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class IPMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
    @Override
    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        byte[] bytes = value.getBytes();
        String log = new String(bytes);
        String[] arrs = log.split("\t");
        String ip = arrs[0];//统计所有IP不做筛选了
        context.write(new Text(ip), new IntWritable(1));
    }
}
