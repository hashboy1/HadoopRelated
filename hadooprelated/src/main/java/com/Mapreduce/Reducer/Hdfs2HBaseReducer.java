package com.Mapreduce.Reducer;

import java.io.IOException;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Hdfs2HBaseReducer extends Reducer<Text, Text, ImmutableBytesWritable, Put> {
        public void reduce(Text rowkey, Iterable<Text> value, Context context) throws IOException,InterruptedException {
               
        	   /*
        	   String k = rowkey.toString();
                for(Text val : value) {
                        Put put = new Put(k.getBytes());
                        String[] strs = val.toString().split(":");
                        String family = strs[0];
                        String qualifier = strs[1];
                        String v = strs[2];
                        put.add(family.getBytes(), qualifier.getBytes(), v.getBytes());
                        context.write(new ImmutableBytesWritable(k.getBytes()), put);
                }
                */
        	 //context.write(new Text(rowkey),value.toString(),);
        }
}