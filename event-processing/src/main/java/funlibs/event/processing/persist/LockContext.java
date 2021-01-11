package funlibs.event.processing.persist;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class LockContext {
	private static final int PARTITIONS = 10241;
	private final Lock[] locks;

	public LockContext() {
		this.locks = new Lock[PARTITIONS];
		for (int i = 0; i < PARTITIONS; i++) {
			this.locks[i] = new ReentrantLock();
		}
	}

	public Lock get(byte[] key) {
		int pos = Math.abs(Arrays.hashCode(key)) % PARTITIONS;
		return locks[pos];
	}
}
