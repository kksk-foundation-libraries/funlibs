package funlibs.queue.persist;

public class PublishTransaction {
	private final PubSubQueue pubSubQueue;
	private final long transactionId;

	public PublishTransaction(PubSubQueue pubSubQueue, long transactionId) {
		this.pubSubQueue = pubSubQueue;
		this.transactionId = transactionId;
	}

	public PublishTransaction publish(long topicId, byte[] key, byte[] value) {
		pubSubQueue.publish(topicId, key, value, transactionId);
		return this;
	}

	public void commit() {
		pubSubQueue.commit(transactionId);
	}
	
	public void rollback() {
		pubSubQueue.rollback(transactionId);
	}
}
