package producer;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.junit.Test;

public class KafkaProducerTest {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "192.168.116.180:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("partitioner.class", "org.apache.kafka.clients.producer.internals.DefaultPartitioner");
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        Producer<String, String> producer = new KafkaProducer<>(props);
        for(int i=0;i<1000;i++){
        	String message = "message"+i;
        	String k = "key" + i;
        	producer.send(new ProducerRecord<String, String>("chensi",message));
        }
	}
	@Test
	public void test(){
		 Properties props = new Properties();
	        props.put("bootstrap.servers", "192.168.116.180:9092");
	        props.put("acks", "all");
	        props.put("retries", 0);
	        props.put("batch.size", 16384);
	        props.put("linger.ms", 1);
	        props.put("buffer.memory", 33554432);
	        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	        Producer<String, String> producer = new KafkaProducer<>(props);
	        String data = "{\"eventId\":25, \"dn\":AAAAAQAAAAEAAAAAAAAABQ==, \"ts\": 1492495190203, \"value\": 1111}";
	        producer.send(new ProducerRecord<String, String>("linlin", "key", data));  
	        System.out.println(data);  
	        System.out.println("·¢ËÍ³É¹¦");
	        producer.close();
	}
	@Test
	public void consumer(){
		 Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.116.180:9092");
        props.put("group.id", "lingroup");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("chensi"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(500);
            for (ConsumerRecord<String, String> record : records){
                System.out.println("offset = {"+record.offset()+"}, key = {"+record.key()+"}, value = {"+record.value()+"}, partiton = {"+record.partition()+"}");
            }
        }
	}
}
