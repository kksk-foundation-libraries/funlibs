package funlibs.concurrent;

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
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
			ConcurrentUtil.sleepSlient(10);
			LOG.debug("before unblock");
			ConcurrentUtil.sleepSlient(100);
			blocker.unblock();
			LOG.debug("after unblock");
		});
		LOG.debug("before blockSilent");
		ConcurrentUtil.sleepSlient(30);
		blocker.blockSilent();
		LOG.debug("after blockSilent");
		ConcurrentUtil.getSilent(future);
	}

	@Test
	public void test002() {
		LOG.debug("test002");
		final Blocker blocker = new Blocker();
		CompletableFuture<Void> future = CompletableFuture.runAsync(() -> {
			ConcurrentUtil.sleepSlient(10);
			LOG.debug("before unblock");
			ConcurrentUtil.sleepSlient(100);
			blocker.unblock();
			LOG.debug("after unblock");
		});
		LOG.debug("before blockSilent");
		ConcurrentUtil.sleepSlient(30);
		blocker.blockSilent(10);
		LOG.debug("after blockSilent");
		ConcurrentUtil.getSilent(future);
	}
}
