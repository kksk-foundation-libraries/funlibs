package funlibs.queue.persist;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.concurrent.PartitionedReadWriteLock;
import funlibs.logging.SimpleLogging;

public class DistributedQueueTest {
	static final SimpleLogging ll = SimpleLogging.of() //
		.loggingGroup("funlibs.queue.persist") //
		.showDateTime() //
		.showShortLogName() //
		.showThreadName() //
		.dateTimeFormat("yyyy-MM-dd HH:mm:ss.SSS") //
		.loggingGroupLogLevel("DEBUG") //
		.initialize() //
	;
	private static final Logger LOG = LoggerFactory.getLogger(DistributedQueueTest.class);

	@Test
	public void test() {
		DistributedQueue queue = new DistributedQueue(new PartitionedReadWriteLock(10241), new LocalQueueStore());
		for (int i = 0; i < 1000; i++) {
			queue.offer(("k" + (i % 10)).getBytes(), ("v" + i).getBytes());
		}
		for (int i = 0; i < 10; i++) {
			byte[] k = ("k" + i).getBytes();
			while (queue.peek(k) != null) {
				byte[] v = queue.poll(k);
				LOG.debug("k:{}, v:{}", new String(k), new String(v));
			}
		}
		//		fail("Not yet implemented");
	}

}
