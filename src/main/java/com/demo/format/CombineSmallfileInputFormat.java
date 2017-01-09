package com.demo.format;

import com.demo.reader.CombineSmallfileRecordReader;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

/**
 * Created by guang_chen on 2016/11/14.
 */

public class CombineSmallfileInputFormat extends CombineFileInputFormat<LongWritable, BytesWritable> {

    @Override
    public RecordReader<LongWritable, BytesWritable> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        CombineFileSplit combineFileSplit = (CombineFileSplit) split;
        CombineFileRecordReader<LongWritable, BytesWritable> recordReader
                = new CombineFileRecordReader<LongWritable, BytesWritable>(combineFileSplit, context, CombineSmallfileRecordReader.class);
        try {
            recordReader.initialize(combineFileSplit, context);
        } catch (InterruptedException e) {
            new RuntimeException("Error to initialize CombineSmallfileRecordReader.");
        }
        return recordReader;
    }

}