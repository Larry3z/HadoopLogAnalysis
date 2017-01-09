package com.demo.driver.hdfs;

import com.demo.format.CombineSmallfileInputFormat;
import com.demo.utils.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.Tool;

import java.io.IOException;

public class SmallFilesToSequenceFileDriver extends Configured
        implements Tool {
    @Override
    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(config);
        boolean flag = fs.exists(outputPath);
        if (flag) {
            fs.delete(outputPath, true);//如果Output路径存在删除其中内容
            System.out.println("-------delete output path: " + outputPath + "-----------");
        }
        Job job = JobBuilder.buildJob(this, config, args);
        if (job == null) {
            return -1;
        }
        job.setInputFormatClass(CombineSmallfileInputFormat.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setMapperClass(SequenceFileMapper.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }


    static class SequenceFileMapper
            extends Mapper<LongWritable, BytesWritable, Text, BytesWritable> {
        private Text filenameKey = new Text();

        @Override
        protected void map(LongWritable key, BytesWritable value, Context context)
                throws IOException, InterruptedException {
            String fileName = context.getConfiguration().get("map.input.file.name");
            filenameKey.set(fileName);
            context.write(filenameKey, value);
        }
    }
}