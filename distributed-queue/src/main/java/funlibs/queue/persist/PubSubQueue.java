package funlibs.queue.persist;

import java.util.concurrent.atomic.AtomicLong;

import funlibs.concurrent.PartitionedReadWriteLock;
import funlibs.queue.model.KeyValue;

public class PubSubQueue {
	private final HashPartitionedQueue mainQueue;
	private final HashPartitionedQueue publishQueue;
	private final AtomicLong publishTransactionId = new AtomicLong();
	private final QueueStore subscribeOffset;

	public PubSubQueue(PartitionedReadWriteLock locks, QueueStore publishStore, QueueStore subscribeStore, int partitions) {
		this.mainQueue = new HashPartitionedQueue(locks, publishStore, partitions);
		this.publishQueue = new HashPartitionedQueue(locks, publishStore, partitions);
		this.subscribeOffset = subscribeStore;
	}

	public long publish(String topic, byte[] key, byte[] value) {
		// TODO
		return 0L;
	}

	public void commit(long transactionId) {
		// TODO

	}

	public KeyValue subscribe(String topic, int partition, int subscriber) {
		// TODO
		return null;
	}
}
