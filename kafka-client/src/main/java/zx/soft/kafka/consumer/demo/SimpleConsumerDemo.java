package zx.soft.kafka.consumer.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.frame.reader.FrameReader;

public class SimpleConsumerDemo {

	private static final Logger logger = LoggerFactory.getLogger(SimpleConsumerDemo.class);

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka01:19092,kafka02:19093,kafka03:19094");
		props.put("group.id", "donglei");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", ByteArrayDeserializer.class.getName());
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
		consumer.subscribe(Arrays.asList("apt-cache"));
		int key = -1;
		while (true) {
			ConsumerRecords<String, byte[]> records = consumer.poll(1000);
			for (ConsumerRecord<String, byte[]> record : records) {
				logger.info("{} {} {} {}", record.partition(), record.offset(), record.key(), record.value().toString());
				FrameReader reader = new FrameReader(record.value());
				logger.info(reader.nextFrame().toString());
			}
		}
	}

}
