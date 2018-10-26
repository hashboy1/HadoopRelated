package com.kafka;
 
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
 
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
 
public class KafkaUtils {
private final static Logger LOGGER = LoggerFactory.getLogger(KafkaUtils.class);

private static Producer<String, String> producer;
private static Consumer<String, String> consumer;

private static String BOOTSTRAP_SERVERS="192.168.0.196:9092";
private static String TOPIC_NAME="test";

private KafkaUtils() {
 
}
 
/**
 * 生产者，注意kafka生产者不能够从代码上生成主题，只有在服务器上用命令生成
 */
static {
Properties props = new Properties();
props.put("bootstrap.servers", BOOTSTRAP_SERVERS);//服务器ip:端口号，集群用逗号分隔
props.put("acks", "all");
props.put("retries", 0);
props.put("batch.size", 16384);
props.put("linger.ms", 1);
props.put("buffer.memory", 33554432);
props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
producer = new KafkaProducer<>(props);
}
 
/**
 * 消费者
 */
static {
Properties props = new Properties();
props.put("bootstrap.servers", BOOTSTRAP_SERVERS);//服务器ip:端口号，集群用逗号分隔
props.put("group.id", "test");
props.put("enable.auto.commit", "true");
props.put("auto.commit.interval.ms", "1000");
props.put("session.timeout.ms", "30000");
props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
consumer = new KafkaConsumer<>(props);
consumer.subscribe(Arrays.asList(TOPIC_NAME));
}
 
/**
 * 发送对象消息 至kafka上,调用json转化为json字符串，应为kafka存储的是String。
 * @param msg
 */
public static void sendMsgToKafka(String msg) {
	
producer.send(new ProducerRecord<String, String>(TOPIC_NAME, String.valueOf(new Date().getTime()),
JSON.toJSONString(LocalDateTime.now()+"   "+msg)));
LOGGER.info("从kafka发送消息是：" + msg);
}
 

/**
 * 从kafka上接收对象消息，将json字符串转化为对象，便于获取消息的时候可以使用get方法获取。
 */
public static void getMsgFromKafka(){
while(true){
ConsumerRecords<String, String> records = KafkaUtils.getKafkaConsumer().poll(100);
LOGGER.info("收到消息：" + records.count());
if (records.count() > 0) {
for (ConsumerRecord<String, String> record : records) {
	LOGGER.info("从kafka接收到的消息是：" + record.value());
/*
JSONObject jsonAlarmMsg = JSON.parseObject(record.value());
String alarmMsg = JSONObject.toJavaObject(jsonAlarmMsg, String.class);
LOGGER.info("从kafka接收到的消息是：" + alarmMsg);
*/

}
}
}
}

public static Consumer<String, String> getKafkaConsumer() {
return consumer;
}
 
public static void closeKafkaProducer() {
producer.close();
}
 
public static void closeKafkaConsumer() {
consumer.close();
}

public static void main(String[] args) throws Exception {
	

	KafkaUtils.sendMsgToKafka("hello kafka");
	KafkaUtils.getMsgFromKafka();

}


}
 
 
