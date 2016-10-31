package zx.soft.kafka.consumer.fileextract;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.apt.extract.core.ExtractCore;
import zx.soft.kafka.consumer.KafkaConsumerRunner;

/**
 *
 * @author donglei
 *
 */
public class FileExtractHandler extends KafkaConsumerRunner<String, byte[]> {

	private static final Logger logger = LoggerFactory.getLogger(FileExtractHandler.class);

	private ExtractCore extractCore;

	public FileExtractHandler(KafkaConsumer<String, byte[]> consumer, ExtractCore extractCore) {
		super(consumer);
		this.extractCore = extractCore;
	}

	@Override
	public void handleMessage(String key, byte[] value) {
		byte[] datas = value;
		ByteBuffer buffer = ByteBuffer.wrap(datas);
		buffer.order(ByteOrder.BIG_ENDIAN);
		if (buffer.remaining() > 32) {

			buffer.position(32);

			// IP标识16byte
			//			byte[] ipDatas = new byte[16];
			//			buffer.get(ipDatas);
			//			boolean netType = false;
			//			for (int i = 0; i < 12; i++) {
			//				byte a = ipDatas[i];
			//				if (a != 0) {
			//					netType = true;
			//					break;
			//				}
			//			}
			//			if (!netType) {
			//				byte[] tmp = new byte[4];
			//				System.arraycopy(ipDatas, 12, tmp, 0, 4);
			//				ipDatas = tmp;
			//			}
			//			try {
			//				String ip = InetAddress.getByAddress(ipDatas).getHostAddress();
			//				long timestamp = buffer.getLong();
			//				long index = buffer.getLong();
			//
			//				logger.info("ip:" + ip + "\t timestamp:" + timestamp + "\tindex: " + index);
			//			} catch (Exception e) {
			//			}

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
