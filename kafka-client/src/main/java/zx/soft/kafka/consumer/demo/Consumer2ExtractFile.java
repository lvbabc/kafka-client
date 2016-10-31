package zx.soft.kafka.consumer.demo;

import java.net.InetAddress;
import java.nio.ByteBuffer;
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

import zx.soft.apt.extract.core.ExtractCore;

public class Consumer2ExtractFile {

	private static final Logger logger = LoggerFactory.getLogger(Consumer2ExtractFile.class);

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka01:19092,kafka02:19093,kafka03:19094");
		props.put("group.id", "aksdjfkasldkf");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put("key.deserializer", StringDeserializer.class.getName());
		props.put("value.deserializer", ByteArrayDeserializer.class.getName());
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
		consumer.subscribe(Arrays.asList("apt-cache"));
		String libDir = "/opt/jchannel/jchannel/lib/jcmodule";
		String outPutDir = "/tmp/output";
		String linkDir = "/tmp/output_link";
		ExtractCore extractCore = new ExtractCore(libDir, outPutDir, linkDir);
		extractCore.initStream();

		while (true) {
			ConsumerRecords<String, byte[]> records = consumer.poll(1000);
			for (ConsumerRecord<String, byte[]> record : records) {
				System.out.printf("partition = %d, offset = %d, key = %s, value = %s\n", record.partition(),
						record.offset(), record.key(), record.value());
				byte[] datas = record.value();
				ByteBuffer buffer = ByteBuffer.wrap(datas);
				if (buffer.remaining() > 32) {

					//					buffer.position(32);

					// IP标识16byte
					byte[] ipDatas = new byte[16];
					buffer.get(ipDatas);
					boolean netType = false;
					for (int i = 0; i < 12; i++) {
						byte a = ipDatas[i];
						if (a != 0) {
							netType = true;
							break;
						}
					}
					if (!netType) {
						byte[] tmp = new byte[4];
						System.arraycopy(ipDatas, 12, tmp, 0, 4);
						ipDatas = tmp;
					}
					try {
						String ip = InetAddress.getByAddress(ipDatas).getHostAddress();
						long timestamp = buffer.getLong();
						long index = buffer.getLong();

						logger.info("ip:" + ip + "\t timestamp:" + timestamp + "\tindex: " + index);
					} catch (Exception e) {
					}

					while (buffer.hasRemaining()) {
						int size = buffer.remaining() > ExtractCore.ONE_BUFFER_SIZE ? ExtractCore.ONE_BUFFER_SIZE : buffer
								.remaining();
						byte[] cache = new byte[size];
						buffer.get(cache);
						extractCore.extract(cache);
					}
				}
			}
		}
	}

}
