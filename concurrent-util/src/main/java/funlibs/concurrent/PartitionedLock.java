package funlibs.concurrent;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class PartitionedLock {
	private final int partitions;
	private final Lock[] locks;

	public PartitionedLock() {
		this(1023);
	}

	public PartitionedLock(int partitions) {
		this.partitions = partitions;
		this.locks = new Lock[partitions];

		for (int i = 0; i < partitions; i++) {
			this.locks[i] = new ReentrantLock();
		}
	}

	public Lock get(byte[] key) {
		int pos = Math.abs(Arrays.hashCode(key)) % partitions;
		return locks[pos];
	}
}
