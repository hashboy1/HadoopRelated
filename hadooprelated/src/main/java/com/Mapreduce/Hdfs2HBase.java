package com.Mapreduce;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.Mapreduce.Mapper.Hdfs2HBaseMapper;
import com.Mapreduce.Reducer.Hdfs2HBaseReducer;



public class Hdfs2HBase {
    public static void main(String[] args) throws Exception {
    	Job job = Job.getInstance();
		job.setJobName("mapreduce job");
	
                
		//下面为了远程提交添加设置：
		Configuration conf = job.getConfiguration();
		conf.set("mapreduce.framework.name", "yarn");
		conf.set("mapreduce.app-submission.cross-platform","true");
		//conf.set("hbase.zookeeper.quorum", "MASTER:2181");
		conf.set("fs.default.name", "hdfs://192.168.0.196:9000");
		conf.set("yarn.resourcemanager.resource-tracker.address", "192.168.0.196:8031");
		conf.set("yarn.resourcemanager.address", "192.168.0.196:8032");
		conf.set("yarn.resourcemanager.scheduler.address", "192.168.0.196:8030");
		conf.set("yarn.resourcemanager.admin.address", "192.168.0.196:8033");
		//conf.set("yarn.application.classpath","$HADOOP_HOME/lib/*");
		/*
		conf.set("yarn.application.classpath", "$HADOOP_CONF_DIR,"
			+"$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
			+"$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
			+"$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,"
			+"$YARN_HOME/*,$YARN_HOME/lib/*,"
			+"$HBASE_HOME/*,$HBASE_HOME/lib/*,$HBASE_HOME/conf/*");
		*/
		conf.set("mapreduce.jobhistory.address", "192.168.0.196:10020");
		conf.set("mapreduce.jobhistory.webapp.address", "192.168.0.196:19888");
		conf.set("mapred.child.java.opts", "-Xmx1024m");
		
		
        job.setJarByClass(Hdfs2HBase.class);
        //job.setJar("E:\\workspace\\HadoopRelated\\hadoop.jar");
        job.setMapperClass(Hdfs2HBaseMapper.class);
        job.setReducerClass(Hdfs2HBaseReducer.class);
        
        
        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        
        //job.setMapOutputKeyClass(Text.class);    
        //job.setMapOutputValueClass(Text.class);  

        //job.setOutputKeyClass(ImmutableBytesWritable.class);
        //job.setOutputValueClass(Put.class);
        

        FileInputFormat.addInputPath(job, new Path("/obj/obj/1.obj"));
    	FileOutputFormat.setOutputPath(job, new Path("/obj2/1.txt"));
    	
    	job.submit();
    	System.out.println("jobId:"+job.getJobID().toString());
    }
    
    

        
  
    
    
    
    
}