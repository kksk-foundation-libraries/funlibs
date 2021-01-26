package funlibs.queue.persist;

import java.util.Arrays;
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
	private final int partitions;

	public PubSubQueue(PartitionedReadWriteLock locks, QueueStore publishStore, QueueStore subscribeStore, int partitions) {
		this.mainQueue = new HashPartitionedQueue(locks, publishStore, partitions);
		this.publishQueue = new DistributedQueue(locks, publishStore);
		this.subscribeOffset = subscribeStore;
		this.partitions = partitions;
	}

	public PublishTransaction newTransaction() {
		return new PublishTransaction(this, publishTransactionId.incrementAndGet());
	}

	public void publish(long topicId, byte[] key, byte[] value) {
		mainQueue.offer(topicId, key, value);
	}

	void publish(long topicId, byte[] key, byte[] value, long transactionId) {
		TransactionKey transactionKey = new TransactionKey().withTransactionId(transactionId);
		TransactionEntry transactionEntry = new TransactionEntry().withTopicId(topicId).withKey(key).withValue(value);
		publishQueue.offer(serde.ser(transactionKey), serde.ser(transactionEntry));
	}

	void commit(long transactionId) {
		TransactionKey transactionKey = new TransactionKey().withTransactionId(transactionId);
		byte[] key = serde.ser(transactionKey);
		KeyValue kv;
		while ((kv = publishQueue.poll(key)) != null) {
			TransactionEntry transactionEntry = serde.des(kv.getValue(), TransactionEntry.class);
			mainQueue.offer(transactionEntry.getTopicId(), transactionEntry.getKey(), transactionEntry.getValue());
		}
	}

	void rollback(long transactionId) {
		TransactionKey transactionKey = new TransactionKey().withTransactionId(transactionId);
		byte[] key = serde.ser(transactionKey);
		while (publishQueue.poll(key) != null) {
		}
	}

	public KeyValue subscribe(long topicId, int partition, int subscriber) {
		byte[] key;
		byte[] lastValue;
		KeyValue next = null;
		SubscribeKey subscribeKey = new SubscribeKey().withTopicId(topicId).withPartition(partition).withSubscriber(subscriber);
		key = serde.ser(subscribeKey);
		lastValue = subscribeOffset.get(key);
		KeyValue last = null;
		if (lastValue != null) {
			last = serde.des(lastValue, KeyValue.class);
			next = mainQueue.next(topicId, last.getKey(), last.getValue(), last.getOffset());
			if (next == null) {
				// has housekeeped.
				lastValue = null;
			}
		}
		if (lastValue == null) {
			MarkKey markKey = new MarkKey().withTopicId(topicId).withPartition(partition);
			lastValue = subscribeOffset.get(serde.ser(markKey));
			if (lastValue != null) {
				last = serde.des(lastValue, KeyValue.class);
			} else {
				last = mainQueue.peek(topicId, partition);
			}
			if (last != null) {
				next = mainQueue.next(topicId, last.getKey(), last.getValue(), last.getOffset());
			}
		}
		if (last != null) {
			subscribeOffset.put(key, serde.ser(last));
		}
		return next;
	}

	public void commitAcknowledge(long topicId, int partition, int subscriber) {
		SubscribeKey subscribeKey = new SubscribeKey().withTopicId(topicId).withPartition(partition).withSubscriber(subscriber);
		byte[] subscribedBin = subscribeOffset.get(serde.ser(subscribeKey));
		if (subscribedBin != null) {
			AcknowledgeKey acknowledgeKey = new AcknowledgeKey().withTopicId(topicId).withPartition(partition).withSubscriber(subscriber);
			subscribeOffset.put(serde.ser(acknowledgeKey), subscribedBin);
		}
	}

	public void rollbackAcknowledge(long topicId, int partition, int subscriber) {
		AcknowledgeKey acknowledgeKey = new AcknowledgeKey().withTopicId(topicId).withPartition(partition).withSubscriber(subscriber);
		byte[] acknowledgedBin = subscribeOffset.get(serde.ser(acknowledgeKey));
		if (acknowledgedBin != null) {
			SubscribeKey subscribeKey = new SubscribeKey().withTopicId(topicId).withPartition(partition).withSubscriber(subscriber);
			subscribeOffset.put(serde.ser(subscribeKey), acknowledgedBin);
		}
	}

	public void mark(long topicId, int partition) {
		MarkKey markKey = new MarkKey().withTopicId(topicId).withPartition(partition);
		KeyValue keyValue = mainQueue.peek(topicId, partition);
		subscribeOffset.put(serde.ser(markKey), serde.ser(keyValue));
	}

	public void houseKeep(long topicId) {
		for (int partition = 0; partition < partitions; partition++) {
			housekeep(topicId, partition);
		}
	}

	public void housekeep(long topicId, int partition) {
		MarkKey markKey = new MarkKey().withTopicId(topicId).withPartition(partition);
		byte[] key = serde.ser(markKey);
		byte[] value;
		value = subscribeOffset.get(key);
		if (value != null) {
			KeyValue markedKeyValue = serde.des(value, KeyValue.class);
			KeyValue keyValue;
			while ((keyValue = mainQueue.peek(topicId, partition)) != null) {
				if (Arrays.equals(keyValue.getKey(), markedKeyValue.getKey()) && Arrays.equals(keyValue.getValue(), markedKeyValue.getValue()) && keyValue.getOffset() == markedKeyValue.getOffset()) {
					subscribeOffset.remove(key);
					break;
				}
				mainQueue.poll(topicId, partition);
			}
		}
	}
}
