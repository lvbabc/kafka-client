package zx.soft.kafka.consumer.digest;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.apt.parser.core.ParserCore;
import zx.soft.frame.domain.Frame;
import zx.soft.frame.reader.FrameReader;
import zx.soft.kafka.consumer.KafkaConsumerRunner;

/**
 * 摘要信息与五元组信息提取
 * @author donglei
 * @date: 2016年5月17日 上午9:59:33
 */
public class TupleDigestHandler extends KafkaConsumerRunner<String, byte[]> {

	private static final Logger logger = LoggerFactory.getLogger(TupleDigestHandler.class);

	private ParserCore parserCore;

	public TupleDigestHandler(KafkaConsumer<String, byte[]> consumer, ParserCore parserCore) {
		super(consumer);
		this.parserCore = parserCore;
	}

	@Override
	public void handleMessage(String key, byte[] value) {
		if (value.length > 32) {
			FrameReader reader = new FrameReader(value);
			Frame frame = reader.nextFrame();
			this.parserCore.parse(frame);
		} else {
			logger.error("Malformed kafka data packet!");
		}
	}
}
