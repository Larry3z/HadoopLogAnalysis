#!/usr/bin/env bash
inputPath=/flume/nginx_log
outputPath=/log_archive
hadoop=/home/hadoop/hadoop-2.6.3/bin
jar_path=/home/hadoop/test.jar
check_date_format=$(echo {$2} | grep '[0-9]\{4\}-[0-9]\{2\}-[0-9]\{2\}')
curDay=$(date "+%Y-%m-%d")

run() {
    if [ ! -n "$2" ]; then
    #没有日期，默认当天
        local input=$inputPath"/"$curDay
        local output=$outputPath"/"$curDay
        check_folder_exist=$(${hadoop}/hdfs dfs -test -e ${input})
        if [ $? -ne 0 ]; then
            echo "对不起今天HDFS没有目录共合并"
        else
            echo "准备合并..."
            #对今天归档统计PV
            $HADOOP_HOME/bin/hadoop jar $jar_path $1 $input $output $curDay
        fi
    elif [ ! -n "$check_date_format" ]; then
        echo "日期$2不符合格式,需要是$curDay这种格式的"
    else
        #有日期
        local input=$inputPath"/"$2
        local output=$outputPath"/"$2
        check_folder_exist=$(${hadoop}/hdfs dfs -test -e ${input})
        if [ $? -ne 0 ]; then
            echo "对不起"$2"这天HDFS没有目录共合并"
        else
            echo "准备合并..."
            #对某一天归档统计PV
            $HADOOP_HOME/bin/hadoop jar $jar_path $1 $input $output $2
        fi
    fi
}

case $1 in
hdfs)
run $1 $2
;;
mysql)
run $1 $2
;;
*)
echo -n "Usage: $0 {hdfs date format like xxxx-xx-xx|mysql date format like xxxx-xx-xx}"
;;
esac
