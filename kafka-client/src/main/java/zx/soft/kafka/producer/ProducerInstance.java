package zx.soft.kafka.producer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.config.ConfigUtil;
import zx.soft.utils.log.LogbackUtil;

/**
 * 线程安全的生产者实例，采用单例实现
 * @author donglei
 * @date: 2016年1月25日 下午7:58:35
 */
public class ProducerInstance {

	private static Logger logger = LoggerFactory.getLogger(ProducerInstance.class);

	private KafkaProducer<String, String> producer;

	private boolean sync;

	private static ProducerInstance instance = new ProducerInstance();

	private ProducerInstance() {
		Properties kafkaProps = ConfigUtil.getProps("kafka.properties");
		logger.info("load properties :" + kafkaProps.toString());
		sync = Boolean.parseBoolean(kafkaProps.getProperty("sync", "false"));

		Properties props = new Properties();

		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProps.getProperty("bootstrap.servers"));

		props.put(ProducerConfig.ACKS_CONFIG, kafkaProps.getProperty("acks", "1"));
		props.put(ProducerConfig.RETRIES_CONFIG, kafkaProps.getProperty("retries", "0"));
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, kafkaProps.getProperty("batch.size", "16384"));
		props.put(ProducerConfig.LINGER_MS_CONFIG, kafkaProps.getProperty("linger.ms", "0"));
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, kafkaProps.getProperty("buffer.memory", "33554432"));
		props.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, kafkaProps.getProperty("timeout.ms", "50000"));
		props.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, kafkaProps.getProperty("max.request.size", "104857600"));

		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		this.producer = new KafkaProducer<String, String>(props);
	}

	public static ProducerInstance getInstance() {
		return instance;
	}

	public void pushRecord(String topic, String record) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, record);
		if (sync) {
			try {
				producer.send(producerRecord).get();
			} catch (InterruptedException | ExecutionException e) {
				logger.error("Records push failed!");
				logger.error(LogbackUtil.expection2Str(e));
			}
		} else {
			producer.send(producerRecord, new PushCallback());
		}
	}

	public void pushRecord(String topic, String key, String record) {
		ProducerRecord<String, String> producerRecord = new ProducerRecord<String, String>(topic, key, record);
		if (sync) {
			try {
				producer.send(producerRecord).get();
			} catch (InterruptedException | ExecutionException e) {
				logger.error("Records push failed!");
				logger.error(LogbackUtil.expection2Str(e));
			}
		} else {
			producer.send(producerRecord, new PushCallback());
		}
	}

	public void pushRecords(String topic, List<String> records) {
		for (String record : records) {
			pushRecord(topic, record);
		}
	}

	public void close() {
		this.producer.close();
	}

	static class PushCallback implements Callback {
		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			if (exception != null) {
				logger.error("Records push failed!");
				logger.error(LogbackUtil.expection2Str(exception));
			}

		}
	}

}
