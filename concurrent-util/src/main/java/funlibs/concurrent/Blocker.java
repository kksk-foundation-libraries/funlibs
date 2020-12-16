package funlibs.concurrent;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Blocker {
	final CountDownLatch latch = new CountDownLatch(1);

	public void unblock() {
		latch.countDown();
	}

	public void block() throws InterruptedException {
		latch.await();
	}

	public void block(long timeoutMillis) throws InterruptedException {
		block(timeoutMillis, TimeUnit.MILLISECONDS);
	}

	public void block(long timeout, TimeUnit unit) throws InterruptedException {
		latch.await(timeout, unit);
	}

	public void blockSilent() {
		try {
			block();
		} catch (InterruptedException e) {
		}
	}

	public void blockSilent(long timeoutMillis) {
		blockSilent(timeoutMillis, TimeUnit.MILLISECONDS);
	}

	public void blockSilent(long timeout, TimeUnit unit) {
		try {
			block(timeout, unit);
		} catch (InterruptedException e) {
		}
	}
}
