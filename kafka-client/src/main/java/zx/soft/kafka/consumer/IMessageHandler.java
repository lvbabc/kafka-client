package zx.soft.kafka.consumer;

/**
 * Kafka消息处理接口
 * @author donglei
 * @date: 2016年1月25日 下午7:05:33
 * @param <K>
 * @param <V>
 */
public interface IMessageHandler<K,V> {

	public void handleMessage(K key, V value);

}
