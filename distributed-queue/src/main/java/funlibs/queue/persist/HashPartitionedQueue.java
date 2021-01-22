package funlibs.queue.persist;

import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.concurrent.PartitionedReadWriteLock;
import funlibs.queue.model.PartitionKey;
import funlibs.queue.model.KeyValue;

public class HashPartitionedQueue {
	private static final Logger LOG = LoggerFactory.getLogger(HashPartitionedQueue.class);
	private final Serde serde = new Serde();

	private final DistributedQueue queue;
	private final int partitions;

	public HashPartitionedQueue(PartitionedReadWriteLock locks, QueueStore store, int partitions) {
		LOG.debug("constructor");
		queue = new DistributedQueue(locks, store);
		this.partitions = partitions;
	}

	public int size(long topicId) {
		int size = 0;
		for (int i = 0; i < partitions; i++) {
			byte[] queueKey = serde.ser(new PartitionKey().withTopicId(topicId).withPartition(i));
			size += queue.size(queueKey);
		}
		return size;
	}

	public int size(long topicId, int partition) {
		byte[] queueKey = serde.ser(new PartitionKey().withTopicId(topicId).withPartition(partition));
		return queue.size(queueKey);
	}

	public void offer(long topicId, byte[] key, byte[] value) {
		byte[] queueKey = serde.ser(new PartitionKey().withTopicId(topicId).withPartition(Math.abs(Arrays.hashCode(key)) % partitions));
		byte[] queueValue = serde.ser(new KeyValue().withKey(key).withValue(value));
		queue.offer(queueKey, queueValue);
	}

	public KeyValue poll(long topicId, int partition) {
		byte[] queueKey = serde.ser(new PartitionKey().withTopicId(topicId).withPartition(partition));
		KeyValue queueValue = queue.poll(queueKey);
		if (queueValue == null || queueValue.getValue() == null) {
			return null;
		}
		return serde.des(queueValue.getValue(), KeyValue.class).withOffset(queueValue.getOffset());
	}

	public KeyValue peek(long topicId, int partition) {
		byte[] queueKey = serde.ser(new PartitionKey().withTopicId(topicId).withPartition(partition));
		KeyValue queueValue = queue.peek(queueKey);
		if (queueValue == null || queueValue.getValue() == null) {
			return null;
		}
		return serde.des(queueValue.getValue(), KeyValue.class).withOffset(queueValue.getOffset());
	}

	public KeyValue next(long topicId, byte[] key, byte[] value, long offset) {
		byte[] queueKey = serde.ser(new PartitionKey().withTopicId(topicId).withPartition(Math.abs(Arrays.hashCode(key)) % partitions));
		byte[] queueValue = serde.ser(new KeyValue().withKey(key).withValue(value));
		KeyValue queueNextValue = queue.next(queueKey, queueValue, offset);
		if (queueNextValue == null || queueNextValue.getValue() == null) {
			return null;
		}
		return serde.des(queueNextValue.getValue(), KeyValue.class).withOffset(queueNextValue.getOffset());
	}
}
