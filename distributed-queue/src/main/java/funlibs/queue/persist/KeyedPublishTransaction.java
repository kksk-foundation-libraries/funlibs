package funlibs.queue.persist;

public class KeyedPublishTransaction {
	private final KeyedPubSubQueue pubSubQueue;
	private final long transactionId;

	public KeyedPublishTransaction(KeyedPubSubQueue pubSubQueue, long transactionId) {
		this.pubSubQueue = pubSubQueue;
		this.transactionId = transactionId;
	}

	public KeyedPublishTransaction publish(byte[] key, byte[] value) {
		pubSubQueue.publish(key, value, transactionId);
		return this;
	}

	public void commit() {
		pubSubQueue.commit(transactionId);
	}
	
	public void rollback() {
		pubSubQueue.rollback(transactionId);
	}
}
