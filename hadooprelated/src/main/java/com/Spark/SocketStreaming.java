package com.Spark;

import java.util.Arrays;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import com.Domain.Person;
import scala.Tuple2;

public class SocketStreaming {

	private static final Log LOG = LogFactory.getLog(SocketStreaming.class);

	public static void main(String[] args) throws Exception {
		
		//String inputfile="hdfs://192.168.0.196:9000/newobj/1.obj";
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		sparkConf.setMaster("local[2]");
		JavaStreamingContext sc =new JavaStreamingContext(sparkConf,Durations.seconds(10));
		
		
		/*
		 * stop the firewall on this port
		 * run"nc -lk port"
		 * input the word split by " " in console 
		 */
        
		JavaReceiverInputDStream<String> lines = sc.socketTextStream("192.168.0.249", 6789);	
		JavaDStream<String> words = lines.flatMap(x -> Arrays.asList(x.split(" ")).iterator());	   //将字符串拆分为list
		JavaDStream<Person> wordss = lines.map(x -> convert(x));	                               //仅仅处理字符串
	
		// Count each word in each batch
		JavaPairDStream<String, Integer> pairs = words.mapToPair(s -> new Tuple2<>(s, 1));         //将list变为键值对 
		JavaPairDStream<String, Integer> wordCounts = pairs.reduceByKey((i1, i2) -> i1 + i2);      //将相同key的value相加

		//reduce is a accumulator for each line,but duration is one second, maybe you just can get one line data, the accumulator can't be ran.
		//system will show the below warning, Block input-0-1556588718800 replicated to only 0 peer(s) instead of 1 peers
		JavaDStream<String> wordsr=lines.reduce((x,y)->{
			System.out.println("x:"+x);
			System.out.println("y:"+y);
			//LOG.warn("x:"+x);
			//LOG.warn("y:"+y);
			return x+y;
	     });    
		
		// Print the first ten elements of each RDD generated in this DStream to the console
		wordCounts.print();		
		//wordss.print();
		wordsr.print();
		//wordCounts.foreachRDD(x->System.out.println(x));

		//开始流处理
		sc.start();
		sc.awaitTermination();
               
       }
	
	
	public static Person convert(String x)
	{
		
		String[] y=x.split(" ");
		if (y.length>=2)
		{	
			String name=y[0];
			int age=0;
			if (StringUtils.isNumeric(y[1])) age=Integer.valueOf(y[1]);						
			Person pp=new Person(name,age);
			return pp;
		}
		else
		{	
		return null;
		}
	}
	

}
