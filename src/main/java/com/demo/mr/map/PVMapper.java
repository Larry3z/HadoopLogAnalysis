package com.demo.mr.map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;


public class PVMapper extends Mapper<Text, BytesWritable, Text, IntWritable> {
    @Override
    public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
        byte[] bytes = value.getBytes();
        String log = new String(bytes);
        String[] arrs = log.split("\t");
        String pageStr = arrs[3];
        String respCode = arrs[4];
        //只取200的，并且不统计css、js和图片等
        //if(respCode.equals("200") && !pageStr.contains("/resource/images") && !pageStr.contains("/resource/js")){
        if(respCode.equals("200") && !pageStr.contains("/resource") && !pageStr.contains("/jshop/resource")&&
                !pageStr.contains("http")){
            String[] pageArrs = pageStr.split(" ");
            Text page = new Text(pageArrs[1]);
            context.write(page, new IntWritable(1));
        }
    }
}
