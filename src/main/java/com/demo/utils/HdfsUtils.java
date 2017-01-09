package com.demo.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.URI;

/**
 * Created by guang_chen on 2016/11/28.
 */
public class HdfsUtils {

    public void CopyFileToHdfsWithProgress(String fromSrc, String destSrc) throws IOException {
        InputStream in = new BufferedInputStream(new FileInputStream(fromSrc));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(destSrc), conf);
        OutputStream out = fs.create(new Path(destSrc), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }
}
