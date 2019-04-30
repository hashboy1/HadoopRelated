package com.kafka.newapi;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class consumer {
	
	public static void main(String[] args) {
		String topic="test";
		Properties props=new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.0.196:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"earliest");
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringDeserializer");
		
		KafkaConsumer<String,String> kc=new KafkaConsumer<String,String>(props);
		kc.subscribe(Collections.singletonList(topic));
		
		
		while(true)
		{
		@SuppressWarnings("deprecation")
		ConsumerRecords<String,String> crs=kc.poll(100);
		for (ConsumerRecord<String,String> cr : crs)
		{
			//System.out.println(cr.partition());
			System.out.println("offset:"+cr.offset()+"-- value:"+cr.value());
		}
		}
	}

}
