package com.Hive;

import java.sql.DriverManager;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
public class HiveTesting {

	
	public static void main(String[] args) {
		
		 try { 
			 
			 
		       // ResourceBundle rb = ResourceBundle.getBundle("config");
		        //Class.forName(rb.getString("hivedriverClassName")).newInstance();
				 
			 /*
			  * it must start the "hive --service metastore" and "hiveserver2" on server side
			  * 
			  */
			 
			     Class.forName("org.apache.hive.jdbc.HiveDriver");
			     String hiveurl="jdbc:hive2://192.168.0.196:10000/default";
			     String hiveusername="root";
			     String hivepassword="";
			     String sql="select model_id,model_name,rank() over(partition by type_id order by model_id ) rp from model_master";
			 
		        Connection conn = DriverManager.getConnection(hiveurl,hiveusername,hivepassword);
		        PreparedStatement pstsm = conn.prepareStatement(sql);	        
		        ResultSet resultSet = pstsm.executeQuery();
		       
		        while(resultSet.next()){
		            String modelID = resultSet.getString(1);
		            String modelName = resultSet.getString(2);
		            String rank = resultSet.getString(3);
		            System.out.println(modelID+"         "+modelName+"         "+rank);
		        }
		       
		    } catch (Exception e) {
		        System.out.println(e);
		     
		    }
		
	}
}
