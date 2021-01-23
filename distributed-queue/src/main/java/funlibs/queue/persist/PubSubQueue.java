package funlibs.queue.persist;

import java.util.concurrent.atomic.AtomicLong;

import funlibs.concurrent.PartitionedReadWriteLock;
import funlibs.queue.model.AcknowledgeKey;
import funlibs.queue.model.KeyValue;
import funlibs.queue.model.MarkKey;
import funlibs.queue.model.SubscribeKey;
import funlibs.queue.model.TransactionEntry;
import funlibs.queue.model.TransactionKey;

public class PubSubQueue {
	private final HashPartitionedQueue mainQueue;
	private final DistributedQueue publishQueue;
	private final AtomicLong publishTransactionId = new AtomicLong();
	private final QueueStore subscribeOffset;
	private final Serde serde = new Serde();

	public PubSubQueue(PartitionedReadWriteLock locks, QueueStore publishStore, QueueStore subscribeStore, int partitions) {
		this.mainQueue = new HashPartitionedQueue(locks, publishStore, partitions);
		this.publishQueue = new DistributedQueue(locks, publishStore);
		this.subscribeOffset = subscribeStore;
	}
	
	public long newTransaction() {
		return publishTransactionId.incrementAndGet();
	}

	public void publish(long topicId, byte[] key, byte[] value, long transactionId) {
		TransactionKey transactionKey = new TransactionKey().withTransactionId(transactionId);
		TransactionEntry transactionEntry = new TransactionEntry().withTopicId(topicId).withKey(key).withValue(value);
		publishQueue.offer(serde.ser(transactionKey), serde.ser(transactionEntry));
	}

	public void commit(long transactionId) {
		TransactionKey transactionKey = new TransactionKey().withTransactionId(transactionId);
		byte[] key = serde.ser(transactionKey);
		KeyValue kv;
		while ((kv = publishQueue.poll(key)) != null) {
			TransactionEntry transactionEntry = serde.des(kv.getValue(), TransactionEntry.class);
			mainQueue.offer(transactionEntry.getTopicId(), transactionEntry.getKey(), transactionEntry.getValue());
		}
	}

	public KeyValue subscribe(long topicId, int partition, int subscriber) {
		// TODO
		byte[] key;
		byte[] value;
		AcknowledgeKey acknowledgeKey = new AcknowledgeKey().withTopicId(topicId).withPartition(partition).withSubscriber(subscriber);
		key = serde.ser(acknowledgeKey);
		value = subscribeOffset.get(key);
		KeyValue keyValue = null;
		if (value != null) {
			keyValue = serde.des(value, KeyValue.class);
		} else {
			MarkKey markKey = new MarkKey().withTopicId(topicId).withPartition(partition);
			value = subscribeOffset.get(serde.ser(markKey));
			if (value != null) {
				keyValue = serde.des(value, KeyValue.class);
			} else {
				keyValue = mainQueue.peek(topicId, partition);
			}
		}
		if (keyValue != null) {
			KeyValue next = mainQueue.next(topicId, keyValue.getKey(), keyValue.getValue(), keyValue.getOffset());
			subscribeOffset.put(serde.ser(new SubscribeKey().withTopicId(topicId).withPartition(partition).withSubscriber(subscriber)), serde.ser(next));
		}
		return keyValue;
	}

	public void acknowledge(long topicId, int partition, int subscriber, KeyValue keyValue) {
		AcknowledgeKey acknowledgeKey = new AcknowledgeKey().withTopicId(topicId).withPartition(partition).withSubscriber(subscriber);
		subscribeOffset.put(serde.ser(acknowledgeKey), serde.ser(keyValue));
	}

	public void mark(long topicId, int partition) {
		MarkKey markKey = new MarkKey().withTopicId(topicId).withPartition(partition);
		KeyValue keyValue = mainQueue.peek(topicId, partition);
		subscribeOffset.put(serde.ser(markKey), serde.ser(keyValue));
	}
}
