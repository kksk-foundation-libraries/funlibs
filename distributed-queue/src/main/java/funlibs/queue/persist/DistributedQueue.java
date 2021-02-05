package funlibs.queue.persist;

import java.util.Arrays;
import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.concurrent.PartitionedReadWriteLock;
import funlibs.queue.model.EntryKey;
import funlibs.queue.model.EntryValue;
import funlibs.queue.model.KeyValue;
import funlibs.queue.model.QueueInfo;
import funlibs.queue.model.QueueKey;
import funlibs.queue.model.QueueNodeInfo;
import funlibs.queue.model.QueueNodeKey;

public class DistributedQueue {
	private static final Logger LOG = LoggerFactory.getLogger(DistributedQueue.class);
	private final Serde serde = new Serde();
	private final QueueStore store;
	private final PartitionedReadWriteLock locks;

	public DistributedQueue(PartitionedReadWriteLock locks, QueueStore store) {
		LOG.debug("constructor");
		this.locks = locks;
		this.store = store;
	}

	public int size(byte[] key) {
		Lock lock = locks.readLock(key);
		try {
			lock.lock();
			QueueInfo queueInfo = getQueueInfo(key);
			return queueInfo.getSize();
		} finally {
			lock.unlock();
		}
	}

	public void offer(byte[] key, byte[] value) {
		Lock lock = locks.writeLock(key);
		try {
			lock.lock();
			QueueInfo queueInfo = getQueueInfo(key);
			long offset = queueInfo.getOffset() + 1;
			queueInfo.setOffset(offset);
			updateEntryValue(key, offset, value);
			long last = queueInfo.getLast();
			QueueNodeInfo currentQueueNodeInfo = getQueueNodeInfo(key, offset);
			queueInfo.setLast(offset);
			currentQueueNodeInfo.setPrev(last);
			if (last == 0L) {
				queueInfo.setFirst(offset);
			} else {
				QueueNodeInfo lastQueueNodeInfo = getQueueNodeInfo(key, last);
				lastQueueNodeInfo.setNext(offset);
				updateQueueNodeInfo(key, last, lastQueueNodeInfo);
			}
			int size = queueInfo.size;
			size++;
			queueInfo.setSize(size);
			updateQueueNodeInfo(key, offset, currentQueueNodeInfo);
			updateQueueInfo(key, queueInfo);
		} finally {
			lock.unlock();
		}
	}

	public KeyValue poll(byte[] key) {
		Lock lock = locks.writeLock(key);
		try {
			lock.lock();
			QueueInfo queueInfo = getQueueInfo(key);
			long first = queueInfo.getFirst();
			if (first == 0L) {
				return null;
			}

			final long curr = first;
			QueueNodeInfo currentNode = getQueueNodeInfo(key, curr);
			first = currentNode.getNext();
			queueInfo.setFirst(first);
			if (first != 0L) {
				QueueNodeInfo firstNode = getQueueNodeInfo(key, first);
				firstNode.withPrev(0L);
				updateQueueNodeInfo(key, first, firstNode);
			}
			removeQueueNodeInfo(key, curr);
			int size = queueInfo.getSize();
			size--;
			queueInfo.setSize(size);
			if (size == 0) {
				removeQueueInfo(key);
			} else {
				updateQueueInfo(key, queueInfo);
			}
			byte[] value = getEntryValue(key, curr);
			removeEntryValue(key, curr);
			return new KeyValue().withKey(key).withValue(value).withOffset(curr);
		} finally {
			lock.unlock();
		}
	}

	public KeyValue peek(byte[] key) {
		Lock lock = locks.readLock(key);
		try {
			lock.lock();
			QueueInfo queueInfo = getQueueInfo(key);
			long first = queueInfo.getFirst();
			if (first == 0L) {
				return null;
			}
			return new KeyValue().withKey(key).withValue(getEntryValue(key, first)).withOffset(first);
		} finally {
			lock.unlock();
		}
	}

	public KeyValue last(byte[] key) {
		Lock lock = locks.readLock(key);
		try {
			lock.lock();
			QueueInfo queueInfo = getQueueInfo(key);
			long last = queueInfo.getLast();
			if (last == 0L) {
				return null;
			}
			return new KeyValue().withKey(key).withValue(getEntryValue(key, last)).withOffset(last);
		} finally {
			lock.unlock();
		}
	}

	public KeyValue next(byte[] key, byte[] value, long offset) {
		Lock lock = locks.readLock(key);
		try {
			lock.lock();
			long current = offset;
			while (current != 0L) {
				byte[] entryValue = getEntryValue(key, current);
				QueueNodeInfo currentNode = getQueueNodeInfo(key, current);
				if (Arrays.equals(value, entryValue)) {
					byte[] nextValue = getEntryValue(key, currentNode.getNext());
					return new KeyValue().withKey(key).withValue(nextValue).withOffset(currentNode.getNext());
				}
				current = currentNode.getNext();
			}
			return null;
		} finally {
			lock.unlock();
		}
	}

	private byte[] queueKey(byte[] key) {
		return serde.ser(new QueueKey().withKey(key));
	}

	private byte[] queueInfo(QueueInfo queueInfo) {
		return serde.ser(queueInfo);
	}

	private QueueInfo getQueueInfo(byte[] key) {
		byte[] val = store.get(queueKey(key));
		if (val == null) {
			return new QueueInfo();
		}
		return serde.des(val, QueueInfo.class);
	}

	private void removeQueueInfo(byte[] key) {
		store.remove(queueKey(key));
	}

	private void updateQueueInfo(byte[] key, QueueInfo queueInfo) {
		store.put(queueKey(key), queueInfo(queueInfo));
	}

	private byte[] queueNodeKey(byte[] key, long offset) {
		return serde.ser(new QueueNodeKey().withKey(key).withOffset(offset));
	}

	private byte[] queueNodeInfo(QueueNodeInfo queueNodeInfo) {
		return serde.ser(queueNodeInfo);
	}

	private QueueNodeInfo getQueueNodeInfo(byte[] key, long offset) {
		if (offset == 0L) {
			return new QueueNodeInfo();
		}
		byte[] val = store.get(queueNodeKey(key, offset));
		if (val == null) {
			return new QueueNodeInfo();
		}
		return serde.des(val, QueueNodeInfo.class);
	}

	private void removeQueueNodeInfo(byte[] key, long offset) {
		store.remove(queueNodeKey(key, offset));
	}

	private void updateQueueNodeInfo(byte[] key, long offset, QueueNodeInfo queueNodeInfo) {
		store.put(queueNodeKey(key, offset), queueNodeInfo(queueNodeInfo));
	}

	private byte[] entryKey(byte[] key, long offset) {
		return serde.ser(new EntryKey().withKey(key).withOffset(offset));
	}

	private byte[] entryValue(byte[] value) {
		return serde.ser(new EntryValue().withValue(value));
	}

	private byte[] getEntryValue(byte[] key, long offset) {
		byte[] val = store.get(entryKey(key, offset));
		if (val == null)
			return null;
		return serde.des(val, EntryValue.class).getValue();
	}

	private void removeEntryValue(byte[] key, long offset) {
		store.remove(entryKey(key, offset));
	}

	private void updateEntryValue(byte[] key, long offset, byte[] value) {
		store.put(entryKey(key, offset), entryValue(value));
	}
}