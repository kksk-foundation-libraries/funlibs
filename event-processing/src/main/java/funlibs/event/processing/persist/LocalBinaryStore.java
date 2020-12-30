package funlibs.event.processing.persist;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.Striped;

import funlibs.binary.Strings;

public class LocalBinaryStore implements BinaryStore {
	private static final Logger LOG = LoggerFactory.getLogger(LocalBinaryStore.class);

	private ConcurrentMap<Binary, byte[]> map = new ConcurrentHashMap<>();
	private final Striped<ReadWriteLock> lockPool = Striped.readWriteLock(10000);
	private final ConcurrentMap<Binary, ReadWriteLock> locks = new ConcurrentHashMap<>();
	private final ConcurrentMap<Binary, AtomicInteger> lockCounter = new ConcurrentHashMap<>();
	private final ReadWriteLock wholeLock = new ReentrantReadWriteLock();
	private final String name;
	private boolean opened = true;

	public LocalBinaryStore(String name) {
		this.name = name;
	}

	private Lock readLock(Binary binKey) {
		ReadWriteLock readWriteLock = locks.computeIfAbsent(binKey, _key -> lockPool.get(_key));
		lockCounter.computeIfAbsent(binKey, _key -> new AtomicInteger()).incrementAndGet();
		return readWriteLock.readLock();
	}

	private Lock writeLock(Binary binKey) {
		ReadWriteLock readWriteLock = locks.computeIfAbsent(binKey, _key -> lockPool.get(_key));
		lockCounter.computeIfAbsent(binKey, _key -> new AtomicInteger()).incrementAndGet();
		return readWriteLock.writeLock();
	}

	private void unlock(Binary binKey, Lock lock) {
		int count = lockCounter.get(binKey).decrementAndGet();
		if (count == 0) {
			lockCounter.remove(binKey);
		}
		lock.unlock();
	}

	private <V> V withReadLock(Binary binKey, Callable<V> callable) {
		Lock whole = wholeLock.readLock();
		Lock lock = readLock(binKey);
		try {
			whole.lock();
			lock.lock();
			return callable.call();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			unlock(binKey, lock);
			whole.unlock();
		}
	}

	private <V> V withWriteLock(Binary binKey, Callable<V> callable) {
		Lock whole = wholeLock.readLock();
		Lock lock = writeLock(binKey);
		try {
			whole.lock();
			lock.lock();
			return callable.call();
		} catch (Exception e) {
			throw new RuntimeException(e);
		} finally {
			unlock(binKey, lock);
			whole.unlock();
		}
	}

	@Override
	public byte[] get(byte[] key) {
		Binary binKey = new Binary(key);
		return withReadLock(binKey, () -> map.get(binKey));
	}

	@Override
	public Map<byte[], byte[]> getAll(Set<? extends byte[]> keys) {
		Map<byte[], byte[]> filtered = new HashMap<>();
		keys.forEach(key -> {
			filtered.put(key, get(key));
		});
		return filtered;
	}

	@Override
	public boolean containsKey(byte[] key) {
		Binary binKey = new Binary(key);
		return withReadLock(binKey, () -> map.containsKey(binKey));
	}

	@Override
	public void put(byte[] key, byte[] value) {
		Binary binKey = new Binary(key);
		withWriteLock(binKey, () -> map.put(binKey, value));
	}

	@Override
	public byte[] getAndPut(byte[] key, byte[] value) {
		Binary binKey = new Binary(key);
		return withWriteLock(binKey, () -> map.put(binKey, value));
	}

	@Override
	public void putAll(Map<? extends byte[], ? extends byte[]> map) {
		Lock whole = wholeLock.writeLock();
		try {
			whole.lock();
			Map<Binary, byte[]> binMap = new HashMap<>();
			map.forEach((k, v) -> {
				binMap.put(new Binary(k), v);
			});
			this.map.putAll(binMap);
		} finally {
			whole.unlock();
		}
	}

	@Override
	public boolean putIfAbsent(byte[] key, byte[] value) {
		Binary binKey = new Binary(key);
		return withWriteLock(binKey, () -> map.putIfAbsent(binKey, value) == null);
	}

	@Override
	public boolean remove(byte[] key) {
		Binary binKey = new Binary(key);
		return withWriteLock(binKey, () -> map.remove(binKey) != null);
	}

	@Override
	public boolean remove(byte[] key, byte[] oldValue) {
		Binary binKey = new Binary(key);
		return withWriteLock(binKey, () -> {
			byte[] previous = map.get(binKey);
			if (Arrays.equals(previous, oldValue)) {
				map.remove(binKey);
				return true;
			}
			return false;
		});
	}

	@Override
	public byte[] getAndRemove(byte[] key) {
		Binary binKey = new Binary(key);
		return withWriteLock(binKey, () -> map.remove(binKey));
	}

	@Override
	public boolean replace(byte[] key, byte[] oldValue, byte[] newValue) {
		Binary binKey = new Binary(key);
		return withWriteLock(binKey, () -> {
			byte[] previous = map.get(binKey);
			if (Arrays.equals(previous, oldValue)) {
				map.put(binKey, newValue);
				return true;
			}
			return false;
		});
	}

	@Override
	public boolean replace(byte[] key, byte[] value) {
		Binary binKey = new Binary(key);
		return withWriteLock(binKey, () -> {
			byte[] previous = map.get(binKey);
			if (previous != null) {
				map.put(binKey, value);
				return true;
			}
			return false;
		});
	}

	@Override
	public byte[] getAndReplace(byte[] key, byte[] value) {
		Binary binKey = new Binary(key);
		return withWriteLock(binKey, () -> map.put(binKey, value));
	}

	@Override
	public void removeAll(Set<? extends byte[]> keys) {
		Lock whole = wholeLock.writeLock();
		try {
			whole.lock();
			keys.forEach(k -> {
				map.remove(new Binary(k));
			});
		} finally {
			whole.unlock();
		}
	}

	@Override
	public void removeAll() {
		Lock whole = wholeLock.writeLock();
		try {
			whole.lock();
			map.clear();
		} finally {
			whole.unlock();
		}
	}

	@Override
	public void clear() {
		Lock whole = wholeLock.writeLock();
		try {
			whole.lock();
			map.clear();
		} finally {
			whole.unlock();
		}
	}

	@Override
	public String getName() {
		return name;
	}

	@Override
	public void close() {
		Lock whole = wholeLock.writeLock();
		try {
			whole.lock();
			opened = false;
		} finally {
			whole.unlock();
		}
	}

	@Override
	public boolean isClosed() {
		Lock whole = wholeLock.readLock();
		try {
			whole.lock();
			return !opened;
		} finally {
			whole.unlock();
		}
	}

	public void dump() {
		map.forEach((k, v) -> {
			LOG.debug("name:{}, key:{}, value:{}", name, Strings.toHex(k.key), Strings.toHex(v));
		});
	}

	private static class Binary {
		private byte[] key;

		public Binary(byte[] key) {
			this.key = key;
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + Arrays.hashCode(key);
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Binary other = (Binary) obj;
			if (!Arrays.equals(key, other.key))
				return false;
			return true;
		}
	}
}
