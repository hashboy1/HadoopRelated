package com.storm;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

public class FirstTopo {
    
    public static void main(String[] args) throws Exception {  
        TopologyBuilder builder = new TopologyBuilder();   
        builder.setSpout("spout", new RandomSpout());  
        builder.setBolt("bolt", new SenqueceBolt()).shuffleGrouping("spout"); 
        Config conf = new Config();  
        conf.setDebug(false); 
        
        
        if (args != null && args.length > 0) { 
        	//submit the job to cluster
            conf.setNumWorkers(3);  
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
        
        } else {  
        	
   	
        	//run job in local memory

            LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("firstTopo", conf, builder.createTopology());  
            Utils.sleep(100000);  
            cluster.killTopology("firstTopo");  
            cluster.shutdown();  
   
       }  
    }  
}