package zx.soft.kafka.main;

import zx.soft.kafka.consumer.digest.TupleDigestConsumerMulti;
import zx.soft.kafka.consumer.digest.TupleDigestConsumerSingle;
import zx.soft.kafka.consumer.fileextract.FileExtractConsumerSingle;
import zx.soft.utils.driver.ProgramDriver;

/**
 * 驱动类
 *
 * @author donglei
 *
 */
public class kafkaRunDriver {

	/**
	 * 主函数
	 */
	public static void main(String[] args) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			// 在hefei09机器上运行
			pgd.addClass("fileExtractConsumerSingle", FileExtractConsumerSingle.class, "single");
			pgd.addClass("tupleDigestConsumerSingle", TupleDigestConsumerSingle.class, "single");
			pgd.addClass("tupleDigestConsumerMulti", TupleDigestConsumerMulti.class, "multi");

			pgd.driver(args);
			// Success
			exitCode = 0;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}

		System.exit(exitCode);

	}

}
