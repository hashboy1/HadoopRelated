package com.Mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.MapFileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;;

//remote submit the job to yarn
public class RemoteMapReduceService {
	public static String startJob() throws Exception {
		Job job = Job.getInstance();
		job.setJobName("first job");
	
                
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
		conf.set("yarn.application.classpath", "$HADOOP_CONF_DIR,"
			+"$HADOOP_COMMON_HOME/*,$HADOOP_COMMON_HOME/lib/*,"
			+"$HADOOP_HDFS_HOME/*,$HADOOP_HDFS_HOME/lib/*,"
			+"$HADOOP_MAPRED_HOME/*,$HADOOP_MAPRED_HOME/lib/*,"
			+"$YARN_HOME/*,$YARN_HOME/lib/*,"
			+"$HBASE_HOME/*,$HBASE_HOME/lib/*,$HBASE_HOME/conf/*");
		conf.set("mapreduce.jobhistory.address", "192.168.0.196:10020");
		conf.set("mapreduce.jobhistory.webapp.address", "192.168.0.196:19888");
		conf.set("mapred.child.java.opts", "-Xmx1024m");
		
	    job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.addInputPath(job, new Path("/obj/obj/1.obj"));
		FileOutputFormat.setOutputPath(job, new Path("/newobj/output.txt"));
		job.waitForCompletion(true);
		
		//job.submit();
		//提交以后，可以拿到JobID。根据这个JobID可以打开网页查看执行进度。
		return job.getJobID().toString();
	}
	
	
	  public static void main(String[] args) throws Exception 
	{
		RemoteMapReduceService.startJob();
	}
}