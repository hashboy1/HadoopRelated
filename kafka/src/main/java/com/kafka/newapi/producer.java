package com.kafka.newapi;

import java.time.LocalDateTime;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

public class producer {
	
	
   public static void main(String[] args) {
	String topic="test";
	Properties props=new Properties();
	props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.0.196:9092");
	props.put(ProducerConfig.ACKS_CONFIG, "all");
	props.put(ProducerConfig.RETRIES_CONFIG, 0);
	props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");
	 
	KafkaProducer<String,String> producer=new KafkaProducer<String,String>(props);
	
	for (int i=0;i<100;i++)
	{
		
		String value=LocalDateTime.now().toString();
		ProducerRecord<String,String> pr=new ProducerRecord<String,String> (topic,value);
		//producer.send(pr);
		producer.send(pr, new Callback(){
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				// TODO Auto-generated method stub
				
				if (exception == null){
				//System.out.println("partition:"+metadata.partition());
				//System.out.println("offset:"+metadata.offset());
				}
				else{
					
					System.out.println("failed");
				}
				
			}
			
		});
		
		
		System.out.println(value);
	}
	producer.close();
}
   
   
	
}
