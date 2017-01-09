package com.demo.driver.db;

import com.demo.mr.reduce.db.PVDBReducer;
import com.demo.mr.map.PVMapper;
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


public class PVDBDriver extends Configured implements Tool {
    public int run(String[] args) throws Exception {
        Configuration config = getConf();
        config.set("nginx.log.time", args[2]);
//        DBConfiguration.configureDB(config, "com.mysql.jdbc.Driver",
//                "jdbc:mysql://127.0.0.1:3306/jeeshop?useUnicode=true&characterEncoding=utf8", "root", "XXXXXX");
        DBConfiguration.configureDB(config, PropertiesUtils.getPropertyByName("jdbc.driver"),
                PropertiesUtils.getPropertyByName("jdbc.url"), PropertiesUtils.getPropertyByName("jdbc.username"), PropertiesUtils.getPropertyByName("jdbc.password"));
        Job job = JobBuilder.buildJob(this, config, args);
        if (job == null) {
            return -1;
        }
        job.setInputFormatClass(SequenceFileInputFormat.class);
        job.setOutputFormatClass(DBOutputFormat.class);
        job.setMapperClass(PVMapper.class);
        job.setReducerClass(PVDBReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        DBOutputFormat.setOutput(job, "hadoop_pv", "time", "url", "pv", "createAt");
        return job.waitForCompletion(true) ? 0 : 1;
    }
}
