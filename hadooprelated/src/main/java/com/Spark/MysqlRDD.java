package com.Spark;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.avro.ipc.specific.Person;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
//import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.api.java.JavaNewHadoopRDD;
import scala.Tuple2;

public class MysqlRDD {

	
	@SuppressWarnings("serial")
	public static void main(String[] args) {
		
		//String inputfile="hdfs://192.168.0.196:9000/newobj/1.obj";
		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		sparkConf.setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(sparkConf);
        @SuppressWarnings("deprecation")
		//SQLContext sqlc=new SQLContext(sc);
	     

        String url = "jdbc:mysql://192.168.0.249:3306/testdb";
        //查找的表名
        String table = "employee";
        //增加数据库的用户名(user)密码(password),指定test数据库的驱动(driver)
        Properties connectionProperties = new Properties();
        connectionProperties.put("user","vrkb");
        connectionProperties.put("password","3dms");
        connectionProperties.put("driver","com.mysql.jdbc.Driver");

        //SparkJdbc读取Postgresql的products表内容
        System.out.println("读取test数据库中的user_test表内容");
        // 读取表中所有数据
        //DataFrame jdbcDF = sqlc.read().jdbc(url,table,connectionProperties).select("*");
        //显示数据
        //jdbcDF.show();

        
        //JavaRDD<String> rowRdd=jdbcDF.toJavaRDD().map(f-> f.toString());
        //rowRdd.foreach(x->System.out.println(x));
        
        
}
}
