package funlibs.concurrent;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.logging.SimpleLogging;

public class BlockerTest {
	static SimpleLogging logconfing = SimpleLogging.forPackageDebug();
	
	static final Logger LOG = LoggerFactory.getLogger(BlockerTest.class);

	@Test
	public void test001() {
		LOG.debug("test001");
		final Blocker blocker = new Blocker();
		final LinkedList<String> result = new LinkedList<>();
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
			ConcurrentUtil.sleepSlient(10);
			result.add("before unblock");
			LOG.debug("before unblock");
			ConcurrentUtil.sleepSlient(100);
			blocker.unblock();
			result.add("after unblock");
			LOG.debug("after unblock");
		});
		result.add("before blockSilent");
		LOG.debug("before blockSilent");
		ConcurrentUtil.sleepSlient(30);
		blocker.blockSilent();
		result.add("after blockSilent");
		LOG.debug("after blockSilent");
		ConcurrentUtil.getSilent(future);
		assertTrue("will be 'before blockSilent'", "before blockSilent".equals(result.get(0)));
		assertTrue("will be 'before unblock'", "before unblock".equals(result.get(1)));
		assertTrue("will be 'after unblock'", "after unblock".equals(result.get(2)));
		assertTrue("will be 'after blockSilent'", "after blockSilent".equals(result.get(3)));
	}

	@Test
	public void test002() {
		LOG.debug("test002");
		final Blocker blocker = new Blocker();
		final LinkedList<String> result = new LinkedList<>();
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
			ConcurrentUtil.sleepSlient(10);
			result.add("before unblock");
			LOG.debug("before unblock");
			ConcurrentUtil.sleepSlient(100);
			blocker.unblock();
			result.add("after unblock");
			LOG.debug("after unblock");
		});
		result.add("before blockSilent");
		LOG.debug("before blockSilent");
		ConcurrentUtil.sleepSlient(30);
		blocker.blockSilent(10);
		result.add("after blockSilent");
		LOG.debug("after blockSilent");
		ConcurrentUtil.getSilent(future);
		assertTrue("will be 'before blockSilent'", "before blockSilent".equals(result.get(0)));
		assertTrue("will be 'before unblock'", "before unblock".equals(result.get(1)));
		assertTrue("will be 'after blockSilent'", "after blockSilent".equals(result.get(2)));
		assertTrue("will be 'after unblock'", "after unblock".equals(result.get(3)));
	}
}
