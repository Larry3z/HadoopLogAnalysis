package com.demo.mr.reduce.db;

import com.demo.model.IP;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;


public class IPDBReducer extends Reducer<Text, IntWritable, IP, NullWritable> {
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

    @Override
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        try {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            Configuration config = context.getConfiguration();
            String time_str = config.get("nginx.log.time");
            java.util.Date time = sdf.parse(time_str);
            IP ip = new IP(time, key.toString(), sum, new java.util.Date());
            context.write(ip, NullWritable.get());
        } catch (ParseException e) {
            e.printStackTrace();
        }
    }
}
