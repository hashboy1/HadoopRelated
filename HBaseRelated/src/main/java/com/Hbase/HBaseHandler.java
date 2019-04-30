package com.Hbase;



import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;




/*
 * the logic detail when socket connected
 * 
 * 
 * 
 */

public class HBaseHandler implements Runnable {
   
	private int j;
	public static Logger logger1 = Logger.getLogger(HBaseHandler.class);
	public Connection connection;
	String tName="tPerformance";
    // 用HBaseconfiguration初始化配置信息是会自动加载当前应用的classpath下的hbase-site.xml
    public static Configuration configuration = HBaseConfiguration.create();
	public HBaseHandler(int argi) throws Exception
	{
		j=argi;	
		configuration.addResource("hbase-site.xml");
        connection = ConnectionFactory.createConnection(configuration);
	}
	
	public void createTable(String tableName , String... cf1)throws Exception{
        Admin admin = connection.getAdmin();
        //HTD需要TableName类型的tableName，创建TableName类型的tableName
        TableName tbName = TableName.valueOf(tableName);
        //判断表述否已存在，不存在则创建表
        if(admin.tableExists(tbName)){
            System.err.println("表" + tableName + "已存在！");
            return;
        }
        //通过HTableDescriptor创建一个HTableDescriptor将表的描述传到createTable参数中
        HTableDescriptor HTD = new HTableDescriptor(tbName);
        //为描述器添加表的详细参数
        for (String cf : cf1){
            // 创建HColumnDescriptor对象添加表的详细的描述
            HColumnDescriptor HCD =new HColumnDescriptor(cf);
            HTD.addFamily(HCD);
        }
        //调用createtable方法创建表
        admin.createTable(HTD);
    }
	
	
	  public void putData() throws Exception{
	        //通过表名获取tbName
	            TableName tbname = TableName.valueOf(tName);
	            //通过connection获取相应的表
	            Table table =connection.getTable(tbname); 
	            //创建Random对象以作为随机参数
	            Random random = new Random();
	            //hbase支持批量写入数据，创建Put集合来存放批量的数据
	            List<Put> batput = new ArrayList<>();
	            //循环10次，创建10组测试数据放入list中
	            for(int i=0;i<1000000;i++){
	                //实例化put对象，传入行键
	                Put put =new Put(Bytes.toBytes("rowkey_"+j*1000+i));
	                //调用addcolum方法，向i簇中添加字段
	                put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("username1"),Bytes.toBytes(UUID.randomUUID().toString()));
	                put.addColumn(Bytes.toBytes("i"), Bytes.toBytes("username2"),Bytes.toBytes(UUID.randomUUID().toString()));
	                
	                List<Put> tempput = new ArrayList<>();
	                //将测试数据添加到list中
	                tempput.add(put);
	                table.put(tempput);
	                System.out.println("rowkey_"+j*1000+i+" inserted!!");
	            }
	            //调用put方法将list中的测试数据写入hbase
	            //table.put(batput);
	            System.err.println("数据插入完成！");
	        }
	
	
	@Override
	public void run() {            
		try{
				
				//this.createTable(tName, "i");
				this.putData();
				
			} 
			
			catch (Exception e) {
				
				e.printStackTrace();
			}
		     finally{
		    	 try {
					connection.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		     }

		
	}
	

}
