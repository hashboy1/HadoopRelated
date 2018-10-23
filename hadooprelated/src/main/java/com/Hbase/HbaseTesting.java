package com.Hbase;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * Hello world!
 *
 */
public class HbaseTesting 
{
	private static Configuration conf = HBaseConfiguration.create();

    static {

      // conf.set("hbase.rootdir", "hdfs://192.168.0.196:9000/hbase");

       // 设置Zookeeper,直接设置IP地址

       //conf.set("hbase.zookeeper.quorum", "master,slave1,slave2");
       
       conf.set("hbase.zookeeper.quorum", "192.168.0.196:2181");

    }



    // 创建表

    public static void createTable(String tablename, String columnFamily) throws Exception {

       Connection connection =ConnectionFactory.createConnection(conf);

       Admin admin = connection.getAdmin();



       TableName tableNameObj = TableName.valueOf(tablename);



       if (admin.tableExists(tableNameObj)) {

           System.out.println("Tableexists!");

           System.exit(0);

       } else {

           HTableDescriptor tableDesc = new HTableDescriptor(TableName.valueOf(tablename));

           tableDesc.addFamily(new HColumnDescriptor(columnFamily));

           admin.createTable(tableDesc);

           System.out.println("createtable success!");

       }

       admin.close();

       connection.close();

    }



    // 删除表

    public static void deleteTable(String tableName) {

       try {

           Connection connection =ConnectionFactory.createConnection(conf);

           Admin admin = connection.getAdmin();

           TableName table = TableName.valueOf(tableName);

           admin.disableTable(table);

           admin.deleteTable(table);

           System.out.println("deletetable " + tableName + "ok.");

       } catch (IOException e) {

           e.printStackTrace();

       }

    }



    // 插入一行记录

   public static void addRecord(String tableName, String rowKey,String family, String qualifier,String value){

       try {

           Connection connection =ConnectionFactory.createConnection(conf);

           Table table = connection.getTable(TableName.valueOf(tableName));

           Put put = new Put(Bytes.toBytes(rowKey));

           put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier), Bytes.toBytes(value));

           put.addColumn(Bytes.toBytes(family),Bytes.toBytes(qualifier), Bytes.toBytes(value));

           table.put(put);

           table.close();

           connection.close();

           System.out.println("insertrecored " + rowKey + " totable " + tableName + "ok.");

       } catch (IOException e) {

           e.printStackTrace();

       }

    }




    	
    	 
    	    public static void main(String[] args) throws IOException {  
    	    	  Configuration conf;  
    	    	     Connection connection;  
    	    	 Admin admin;  
    	    	  
    	  
    	        conf = HBaseConfiguration.create();  
    	        conf.set("hbase.master", "192.168.0.196:16000");  
    	          
    	        connection = ConnectionFactory.createConnection(conf);  
    	        admin = connection.getAdmin();  
    	          
    	        HTableDescriptor table = new HTableDescriptor(TableName.valueOf("table1"));  
    	        table.addFamily(new HColumnDescriptor("group1")); //创建表时至少加入一个列组  
    	          
    	        if(admin.tableExists(table.getTableName())){  
    	            admin.disableTable(table.getTableName());  
    	            admin.deleteTable(table.getTableName());  
    	        }  
    	        admin.createTable(table);  
    	    
           }   
}
