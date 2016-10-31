package zx.soft.kafka.consumer.demo;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.CompressionType;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class PcapConsumerExample {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "kafka01:9092,kafka02:9092,kafka03:9092");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "10000");
		props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, "1000");

		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

		String uri = "2016-01-26";
		Configuration conf = new Configuration();
		SequenceFile.Writer writer = null;
		LongWritable key = new LongWritable();
		BytesWritable value = new BytesWritable();

		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props);
		AtomicLong count = new AtomicLong(0);
		consumer.subscribe(Arrays.asList("apt-test"));
		try {
			writer = SequenceFile.createWriter(conf, Writer.file(new Path(uri)), Writer.keyClass(LongWritable.class),
					Writer.valueClass(BytesWritable.class), Writer.bufferSize(1024),Writer.compression(CompressionType.NONE));
			for (int i = 0; i < 100; i++) {
				ConsumerRecords<String, byte[]> records = consumer.poll(100);
				System.err.println(i + ": " + records.count());
				for (ConsumerRecord<String, byte[]> record : records) {
					System.out.printf("offset = %d, key = %s, value = %d", record.offset(), record.key(), record.value().length);
					System.out.println();
					key.set(count.get());
					value.set(record.value(), 0, record.value().length);
					writer.append(key, value);
					count.incrementAndGet();
				}
				System.out.println(count.get());
			}
		} catch(Exception e) {

		}finally {
			IOUtils.closeStream(writer);
		}

		consumer.close();

		System.err.println("Finish!");
	}

}
