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

public class KeyedPubSubQueue {
	private final DistributedQueue mainQueue;
	private final DistributedQueue publishQueue;
	private final AtomicLong publishTransactionId = new AtomicLong();
	private final QueueStore subscribeOffset;
	private final Serde serde = new Serde();

	public KeyedPubSubQueue(PartitionedReadWriteLock locks, QueueStore publishStore, QueueStore subscribeStore) {
		this.mainQueue = new DistributedQueue(locks, publishStore);
		this.publishQueue = new DistributedQueue(locks, publishStore);
		this.subscribeOffset = subscribeStore;
	}

	public KeyedPublishTransaction newTransaction() {
		return new KeyedPublishTransaction(this, publishTransactionId.incrementAndGet());
	}

	public void publish(byte[] key, byte[] value) {
		mainQueue.offer(key, value);
	}

	void publish(byte[] key, byte[] value, long transactionId) {
		TransactionKey transactionKey = new TransactionKey().withTransactionId(transactionId);
		TransactionEntry transactionEntry = new TransactionEntry().withKey(key).withValue(value);
		publishQueue.offer(serde.ser(transactionKey), serde.ser(transactionEntry));
	}

	void commit(long transactionId) {
		TransactionKey transactionKey = new TransactionKey().withTransactionId(transactionId);
		byte[] key = serde.ser(transactionKey);
		KeyValue kv;
		while ((kv = publishQueue.poll(key)) != null) {
			TransactionEntry transactionEntry = serde.des(kv.getValue(), TransactionEntry.class);
			mainQueue.offer(transactionEntry.getKey(), transactionEntry.getValue());
		}
	}

	void rollback(long transactionId) {
		TransactionKey transactionKey = new TransactionKey().withTransactionId(transactionId);
		byte[] key = serde.ser(transactionKey);
		while (publishQueue.poll(key) != null) {
		}
	}

	public KeyValue subscribe(byte[] key, int subscriber) {
		byte[] lastValue;
		KeyValue next = null;
		SubscribeKey subscribeKey = new SubscribeKey().withKey(key).withSubscriber(subscriber);
		byte[] subscribeKeyBin = serde.ser(subscribeKey);
		lastValue = subscribeOffset.get(subscribeKeyBin);
		KeyValue last = null;
		if (lastValue != null) {
			last = serde.des(lastValue, KeyValue.class);
			next = mainQueue.next(last.getKey(), last.getValue(), last.getOffset());
			if (next == null) {
				// has housekeeped.
				lastValue = null;
			}
		}
		if (lastValue == null) {
			MarkKey markKey = new MarkKey().withKey(key);
			lastValue = subscribeOffset.get(serde.ser(markKey));
			if (lastValue != null) {
				last = serde.des(lastValue, KeyValue.class);
			} else {
				last = mainQueue.peek(key);
			}
			if (last != null) {
				next = mainQueue.next(last.getKey(), last.getValue(), last.getOffset());
			}
		}
		if (last != null) {
			subscribeOffset.put(subscribeKeyBin, serde.ser(last));
		}
		return next;
	}

	public void commitAcknowledge(byte[] key, int subscriber) {
		SubscribeKey subscribeKey = new SubscribeKey().withKey(key).withSubscriber(subscriber);
		byte[] subscribedBin = subscribeOffset.get(serde.ser(subscribeKey));
		if (subscribedBin != null) {
			AcknowledgeKey acknowledgeKey = new AcknowledgeKey().withKey(key).withSubscriber(subscriber);
			subscribeOffset.put(serde.ser(acknowledgeKey), subscribedBin);
		}
	}

	public void rollbackAcknowledge(byte[] key, int subscriber) {
		AcknowledgeKey acknowledgeKey = new AcknowledgeKey().withKey(key).withSubscriber(subscriber);
		byte[] acknowledgedBin = subscribeOffset.get(serde.ser(acknowledgeKey));
		if (acknowledgedBin != null) {
			SubscribeKey subscribeKey = new SubscribeKey().withKey(key).withSubscriber(subscriber);
			subscribeOffset.put(serde.ser(subscribeKey), acknowledgedBin);
		}
	}

	public void mark(byte[] key) {
		MarkKey markKey = new MarkKey().withKey(key);
		KeyValue keyValue = mainQueue.last(key);
		subscribeOffset.put(serde.ser(markKey), serde.ser(keyValue));
	}

	public void housekeep(byte[] key) {
		MarkKey markKey = new MarkKey().withKey(key);
		byte[] markKeyBin = serde.ser(markKey);
		byte[] value;
		value = subscribeOffset.get(markKeyBin);
		if (value != null) {
			KeyValue markedKeyValue = serde.des(value, KeyValue.class);
			KeyValue keyValue;
			while ((keyValue = mainQueue.peek(key)) != null) {
				if (Arrays.equals(keyValue.getValue(), markedKeyValue.getValue()) && keyValue.getOffset() == markedKeyValue.getOffset()) {
					subscribeOffset.remove(markKeyBin);
					break;
				}
				mainQueue.poll(key);
			}
		}
	}
}
