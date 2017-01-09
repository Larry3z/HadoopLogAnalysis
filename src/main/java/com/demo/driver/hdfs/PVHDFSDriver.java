package com.demo.driver.hdfs;

import com.demo.mr.map.PVMapper;
import com.demo.mr.reduce.hdfs.PVHDFSReducer;
import com.demo.utils.JobBuilder;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;


public class PVHDFSDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        Path outputPath = new Path(args[1]);
        FileSystem fs = FileSystem.get(config);
        boolean flag = fs.exists(outputPath);
        if (flag) {
            fs.delete(outputPath, true);//如果Output路径存在删除其中内容
            System.out.println("#################Delete PV output path: " + outputPath + "#################");
        }
        Job job = JobBuilder.buildJob(this, config, args);
        if (job == null) {
            return -1;
        }
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setMapperClass(PVMapper.class);
        job.setReducerClass(PVHDFSReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
