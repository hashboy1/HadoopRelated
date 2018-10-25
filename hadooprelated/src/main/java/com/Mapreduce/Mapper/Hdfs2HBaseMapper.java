package com.Mapreduce.Mapper;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Hdfs2HBaseMapper extends Mapper<LongWritable, Text, Text, Text> {
    public void map(LongWritable key, Text line, Context context) throws IOException,InterruptedException {
            String lineStr = line.toString();
            
            System.out.println("line:"+lineStr.toString());
            System.out.println("context:"+context.toString());
            /*
            int index = lineStr.indexOf(":");
            String rowkey = lineStr.substring(0, index);
            String left = lineStr.substring(index+1);
        */
            context.write(new Text(lineStr), new Text(lineStr));
    }
}