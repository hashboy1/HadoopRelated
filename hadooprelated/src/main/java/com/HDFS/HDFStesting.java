package com.HDFS;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;

public class HDFStesting {

	public static void main(String[] args) throws Exception {
		//HDFStesting.donwloadfile();
		HDFStesting.copyfile();
	
	
	}
    
	
	
	
	
	
	public static void donwloadfile() throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.0.196:9000"),new Configuration());
        InputStream is = fs.open(new Path("/obj/obj/1.obj")); 
        OutputStream out = new FileOutputStream("F://1.obj");
        IOUtils.copyBytes(is,out,4096,true);
        System.out.println("下载完成");

	}

	public static void copyfile() throws Exception {
		FileSystem fs = FileSystem.get(new URI("hdfs://192.168.0.196:9000"),new Configuration());
        InputStream is = fs.open(new Path("/obj/obj/1.obj"));    
        OutputStream out = fs.create(new Path("/newobj/1.obj"), true);
        IOUtils.copyBytes(is,out,4096,true);
        System.out.println("复制完成");

	}
	
	
}



