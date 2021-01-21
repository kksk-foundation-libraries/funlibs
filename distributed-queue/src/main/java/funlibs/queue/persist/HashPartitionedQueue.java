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
		byte[] queueValue = queue.poll(queueKey);
		if (queueValue == null) {
			return null;
		}
		return serde.des(queueValue, KeyValue.class);
	}
	
	public KeyValue peek(long topicId, int partition) {
		byte[] queueKey = serde.ser(new PartitionKey().withTopicId(topicId).withPartition(partition));
		byte[] queueValue = queue.peek(queueKey);
		if (queueValue == null) {
			return null;
		}
		return serde.des(queueValue, KeyValue.class);
	}
}
