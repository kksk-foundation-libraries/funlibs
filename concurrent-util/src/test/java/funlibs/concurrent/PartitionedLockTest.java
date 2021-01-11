package funlibs.concurrent;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.Lock;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.logging.SimpleLogging;

public class PartitionedLockTest {
	static final SimpleLogging ll = SimpleLogging.forPackageDebug();
	private static final Logger LOG = LoggerFactory.getLogger(PartitionedLockTest.class);

	@Test
	public void test() {
		final int TIMES = 10;
		final int PARTITIONS = 3;
		final ConcurrentMap<String, LinkedList<Integer>> result = new ConcurrentHashMap<>();
		final CountDownLatch latch = new CountDownLatch(TIMES);
		final PartitionedLock locks = new PartitionedLock(PARTITIONS);
		ExecutorService tp = Executors.newFixedThreadPool(TIMES);
		for (int i = 0; i < TIMES; i++) {
			final int index = i;
			tp.submit(() -> {
				final String lockKey = "" + (index % PARTITIONS);
				Lock lock = locks.get(lockKey.getBytes());
				try {
					ConcurrentUtil.sleepSlient(index * 10);
					LOG.debug("{} start lock. lockKey:{}, hashCode:{}, wait:{}", index, lockKey, lock.hashCode(), index * 10);
					lock.lock();
					ConcurrentUtil.sleepSlient((TIMES - index) * 10 + index);
					LOG.debug("{} end lock. wait:{}", index, (TIMES - index) * 10 + index);
				} finally {
					lock.unlock();
				}
				result.computeIfAbsent(lockKey, _k -> new LinkedList<>()).addLast(index);
				latch.countDown();
			});
		}
		ConcurrentUtil.awaitSilent(latch);
		result.forEach((k, l) -> {
			int last = -1;
			for (Integer x : l) {
				LOG.debug("lockKey:{}, last:{}, x.intValue():{}", k, last, x.intValue());
				assertTrue("last < x.intValue()", last < x.intValue());
				last = x.intValue();
			}
		});
	}

}
