package funlibs.queue.persist;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.concurrent.PartitionedReadWriteLock;
import funlibs.logging.SimpleLogging;
import funlibs.queue.model.KeyValue;

public class HashPartitionedQueueTest {
	static final SimpleLogging ll = SimpleLogging.of() //
		.loggingGroup("funlibs.queue.persist") //
		.showDateTime() //
		.showShortLogName() //
		.showThreadName() //
		.dateTimeFormat("yyyy-MM-dd HH:mm:ss.SSS") //
		.loggingGroupLogLevel("DEBUG") //
		.initialize() //
	;
	private static final Logger LOG = LoggerFactory.getLogger(HashPartitionedQueueTest.class);

	@Test
	public void test_001() {
		HashPartitionedQueue queue = new HashPartitionedQueue(new PartitionedReadWriteLock(10241), new LocalQueueStore(), 3);
		for (int i = 0; i < 1000; i++) {
			queue.offer(1, ("k" + (i % 10)).getBytes(), ("v" + i).getBytes());
		}
		for (int i = 0; i < 10; i++) {
			while (queue.peek(1, i) != null) {
				KeyValue v = queue.poll(1, i);
				LOG.debug("partition:{}, k:{}, v:{}", i, new String(v.getKey()), new String(v.getValue()));
			}
		}
	}

	@Test
	public void test_002() {
		HashPartitionedQueue queue = new HashPartitionedQueue(new PartitionedReadWriteLock(10241), new LocalQueueStore(), 3);
		for (int i = 0; i < 1000; i++) {
			queue.offer(1, ("k" + (i % 10)).getBytes(), ("v" + i).getBytes());
		}
		for (int i = 0; i < 10; i++) {
			KeyValue last = queue.peek(1, i);
			if (last == null)
				continue;
			KeyValue next;
			while ((next = queue.next(1, last.getKey(), last.getValue(), last.getOffset())) != null) {
				LOG.debug("k:{}, v:{}, next.k:{}, next.v:{}", new String(last.getKey()), new String(last.getValue()), new String(next.getKey()), new String(next.getValue()));
				last = next;
			}

		}
	}

}
