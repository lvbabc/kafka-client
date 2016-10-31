package zx.soft.kafka.consumer.digest;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.apt.parser.core.ParserCore;
import zx.soft.utils.config.ConfigUtil;

/**
 * @author donglei
 */
public class TupleDigestConsumerSingle {

	private static final Logger logger = LoggerFactory.getLogger(TupleDigestConsumerSingle.class);

	public static void main(String[] args) {
		Properties kafkaProps = ConfigUtil.getProps("digest.properties");
		logger.info("load properties :" + kafkaProps.toString());

		Properties props = new Properties();

		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				kafkaProps.getProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG));
		props.put(ConsumerConfig.GROUP_ID_CONFIG, kafkaProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG));

		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "2000");
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, kafkaProps.getProperty(
				ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName()));
		props.put(
				ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				kafkaProps.getProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
						StringDeserializer.class.getName()));
		props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, kafkaProps.getProperty(ConsumerConfig.FETCH_MIN_BYTES_CONFIG));
		props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG,
				kafkaProps.getProperty(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG));

		String topic = kafkaProps.getProperty("topic");
		String parts = kafkaProps.getProperty("partitions");
		List<TopicPartition> partitions = new ArrayList<>();
		for (String part : parts.split(",")) {
			partitions.add(new TopicPartition(topic, Integer.parseInt(part)));
		}

		logger.info("partitions : " + partitions);
		KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<String, byte[]>(props);
		consumer.assign(partitions);
		ParserCore parserCore = new ParserCore();
		new TupleDigestHandler(consumer, parserCore).run();
	}

}
