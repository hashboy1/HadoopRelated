package com.Spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.Domain.Person;

import kafka.serializer.StringDecoder;
import scala.Tuple2;

public class KafkaStreaming {

	private static final Log LOG = LogFactory.getLog(KafkaStreaming.class);

	public static void main(String[] args) throws Exception {
		
		//String inputfile="hdfs://192.168.0.196:9000/newobj/1.obj";
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		sparkConf.setMaster("local[2]");
		JavaStreamingContext sc =new JavaStreamingContext(sparkConf,Durations.seconds(1));
		
		Map<String, String> kafkaParameters = new HashMap<String, String>();
		kafkaParameters.put("metadata.broker.list", "192.168.0.196:9092");
		
        HashSet<String> topics = new HashSet<String>();
		topics.add("test");
		topics.add("test1");
		
		JavaPairInputDStream<String,String> lines = KafkaUtils.createDirectStream(sc,
		String.class,String.class,StringDecoder.class,StringDecoder.class,kafkaParameters,topics);
        JavaDStream<String> messages=lines.map(f->f._2);
        
      
        
        //print the content
	    //lines.print(); 
		
		//lines.foreachRDD(l->l.foreach(System.out::println));
        messages.foreachRDD(l->l.foreach(System.out::println));
	    
	    
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
