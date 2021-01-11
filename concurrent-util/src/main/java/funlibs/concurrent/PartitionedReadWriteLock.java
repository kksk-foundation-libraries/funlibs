package funlibs.concurrent;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PartitionedReadWriteLock {
	private final int partitions;
	private final ReadWriteLock[] locks;

	public PartitionedReadWriteLock() {
		this(1023);
	}

	public PartitionedReadWriteLock(int partitions) {
		this.partitions = partitions;
		this.locks = new ReadWriteLock[partitions];

		for (int i = 0; i < partitions; i++) {
			this.locks[i] = new ReentrantReadWriteLock();
		}
	}

	public Lock readLock(byte[] key) {
		int pos = Math.abs(Arrays.hashCode(key)) % partitions;
		return locks[pos].readLock();
	}

	public Lock writeLock(byte[] key) {
		int pos = Math.abs(Arrays.hashCode(key)) % partitions;
		return locks[pos].writeLock();
	}
}
