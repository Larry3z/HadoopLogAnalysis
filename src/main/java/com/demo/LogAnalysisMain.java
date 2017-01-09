package com.demo;

import com.demo.driver.db.IPDBDriver;
import com.demo.driver.db.PVDBDriver;
import com.demo.driver.hdfs.IPHDFSDriver;
import com.demo.driver.hdfs.PVHDFSDriver;
import com.demo.driver.hdfs.SmallFilesToSequenceFileDriver;
import com.demo.utils.PropertiesUtils;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/*
*还需要改2016-11-25
 */
public class LogAnalysisMain {
    public static void main(String[] args) throws Exception {
        String type = args[0];
        String[] arr = {args[1],args[2],args[3]};
        int exitCode = ToolRunner.run(new SmallFilesToSequenceFileDriver(), arr);
        //0合并文件成功
        if (exitCode == 0) {
            //由于没有reducer只会生成一个文件，不知道当文件的大小大于hdfs block size时会不会有两个文件，这个留着以后去测试
            String combineResultFilePath = arr[1] + "/part-r-00000";//合并后的文件绝对路径
            String dateStr = arr[2];
            String pvResultPath = PropertiesUtils.getPropertyByName("pv.hdfs.path") + dateStr;
            String pvArgs[] = {combineResultFilePath, pvResultPath, dateStr};
            //这块先这么样吧
            Tool tool1 = type.equalsIgnoreCase("hdfs") ? new PVHDFSDriver() : new PVDBDriver();
            exitCode = ToolRunner.run(tool1, pvArgs);
            if (exitCode == 0) {
                String ipResultPath = PropertiesUtils.getPropertyByName("ip.hdfs.path") + dateStr;
                String ipArgs[] = {combineResultFilePath, ipResultPath, dateStr};
                Tool tool2 = type.equalsIgnoreCase("hdfs") ? new IPHDFSDriver() : new IPDBDriver();
                exitCode = ToolRunner.run(tool2, ipArgs);
            }
        }
        System.exit(exitCode);
    }
}
