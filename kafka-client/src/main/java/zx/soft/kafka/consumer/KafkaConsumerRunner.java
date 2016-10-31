package zx.soft.kafka.consumer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *	Kafka抽象消费者
 * @author donglei
 * @date: 2016年1月25日 下午7:27:10
 * @param <K>
 * @param <V>
 */
public abstract class KafkaConsumerRunner<K, V> implements Runnable, IMessageHandler<K, V> {

	private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerRunner.class);
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final KafkaConsumer<K, V> consumer;

	public KafkaConsumerRunner(KafkaConsumer<K, V> consumer) {
		this.consumer = consumer;
	}

	@Override
	public void run() {
		try {
			while (!closed.get()) {
				logger.info("Wait 1s for getting next record!");
				ConsumerRecords<K,V> records = consumer.poll(1000);
				for(ConsumerRecord<K, V> record : records) {
					logger.info("partition = {}, offset = {}, key = {}, value = {}", record.partition(),
							record.offset(), record.key(), record.value().toString());
					handleMessage(record.key(), record.value());
				}
				consumer.commitSync();
			}
		} catch (WakeupException e) {
			// Ignore exception if closing
			if (!closed.get()) {
				throw e;
			}
		} finally {
			consumer.close();
		}
	}

	// Shutdown hook which can be called from a separate thread
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}

	@Override
	public abstract void handleMessage(K key, V value);
}
