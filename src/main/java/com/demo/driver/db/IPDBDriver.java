package com.demo.driver.db;

import com.demo.mr.map.IPMapper;
import com.demo.mr.reduce.db.IPDBReducer;
import com.demo.utils.JobBuilder;
import com.demo.utils.PropertiesUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.util.Tool;


public class IPDBDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        config.set("nginx.log.time", args[2]);
        //这个要放在buildjob前，不然config没存进去
        DBConfiguration.configureDB(config, PropertiesUtils.getPropertyByName("jdbc.driver"),
                PropertiesUtils.getPropertyByName("jdbc.url"), PropertiesUtils.getPropertyByName("jdbc.username"), PropertiesUtils.getPropertyByName("jdbc.password"));
        Job job = JobBuilder.buildJob(this, config, args);
        if (job == null) {
            return -1;
        }
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        job.setMapperClass(IPMapper.class);
        job.setReducerClass(IPDBReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        DBOutputFormat.setOutput(job, "hadoop_ip", "time", "ip", "num", "createAt");
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
