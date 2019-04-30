package com.Spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import scala.Tuple2;

public class FirstRDD {

	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		String inputfile="hdfs://192.168.0.196:9000/newobj/1.obj";
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		sparkConf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
            
	      List<String> list=new ArrayList<String>();
	      list.add("error:a");
	      list.add("error:b");
	      list.add("error:c");
	      list.add("warning:d");
	      list.add("hadppy ending!");
	      //将列表转换为RDD对象
	      JavaRDD<String> lines = sc.parallelize(list);
	      //System.out.println("original list");
	      //lines.foreach(x -> System.out.println(x));      
	      //将RDD对象lines中有error的表项过滤出来，放在RDD对象errorLines中
	      JavaRDD<String> errorLines = lines.filter(x -> x.contains("error"));
	      //System.out.println("filter list");
	      //errorLines.foreach(System.out::println);    //特有的打印方法
	      JavaRDD<String> objfile=sc.textFile(inputfile);
	      //System.out.println("obfile content");
	      //objfile.foreach(System.out::println);	      
	      JavaRDD<String> filterobjfile=objfile.filter(x -> (x.length()>0 &&x.substring(0,1).equals("f")));
	      /*
	      objfile.filter(new Function<String, Boolean>(){
			@Override
			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return (x.length()>0 &&x.substring(0,1).equals("f"));
			}
	    	  
	      });
	      */
	      
	      
	      
	      //filterobjfile.foreach(System.out::println);
          //filterobjfile.map(filterobjfile => filterobjfile.splits("\t"));
	      
	      
	      
	      //Coding by traditional solution
	      /*
	      JavaRDD<String> mapobjfile=filterobjfile.map(new Function<String, String>() {

			@Override
			public String call(String v1) throws Exception {
				// TODO Auto-generated method stub		
				return v1.replace("\t", "").replace(" ", "");
			}	  
	    	  }	);
	      */
	      
	      //Coding by the lambda expression
	      JavaRDD<String> mapobjfile=filterobjfile.map(y -> y.replace("\t", "").replace(" ", ""));
	      //mapobjfile.foreach(System.out::println);
	      
	      List<Tuple2<String, String>> studentsList = Arrays.asList(
	              new Tuple2<String, String>("class1","leo"),
	              new Tuple2<String, String>("class2","jack"),
	              new Tuple2<String, String>("class1","marry"),
	              new Tuple2<String, String>("class2","tom"),
	              new Tuple2<String, String>("class2","david"));	   
	      JavaPairRDD<String, String> studentsRDD = sc.parallelizePairs(studentsList);
	     // Map<String, Object> studentsCounts = studentsRDD.countByKey();  
	      //studentsCounts.forEach((x,y)-> System.out.println(x+"      "+y));
	      
	      


	}
}
