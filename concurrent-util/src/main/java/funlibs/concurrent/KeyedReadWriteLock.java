package funlibs.concurrent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import lombok.Data;
import lombok.experimental.Accessors;

public class KeyedReadWriteLock {
	private final PooledObjectFactory<ReadWriteLock> factory = new BasePooledObjectFactory<ReadWriteLock>() {
		@Override
		public ReadWriteLock create() throws Exception {
			return new ReentrantReadWriteLock();
		}

		@Override
		public PooledObject<ReadWriteLock> wrap(ReadWriteLock value) {
			return new DefaultPooledObject<>(value);
		}
	};
	private final ObjectPool<ReadWriteLock> pool;

	private final ConcurrentMap<Binary, AtomicInteger> counter = new ConcurrentHashMap<>();
	private final ConcurrentMap<Binary, ReadWriteLock> locks = new ConcurrentHashMap<>();

	@Data
	@Accessors(fluent = true)
	private static class Binary {
		byte[] data;
	}

	public KeyedReadWriteLock() {
		this(1024, 512, 1024);
	}

	public KeyedReadWriteLock(int maxTotal, int minIdle, int maxIdle) {
		GenericObjectPoolConfig<ReadWriteLock> cfg = new GenericObjectPoolConfig<>();
		cfg.setMaxIdle(maxIdle);
		cfg.setMinIdle(minIdle);
		cfg.setMaxTotal(maxTotal);
		pool = new GenericObjectPool<>(factory, cfg);
	}

	public Lock readLock(byte[] key) {
		return lock(key).readLock();
	}

	public Lock writeLock(byte[] key) {
		return lock(key).writeLock();
	}

	private ReadWriteLock lock(byte[] key) {
		return locks.computeIfAbsent(new Binary().data(key), this::getOrCreate);
	}

	private ReadWriteLock getOrCreate(Binary key) {
		counter.computeIfAbsent(key, _key -> new AtomicInteger()).incrementAndGet();
		try {
			return pool.borrowObject();
		} catch (Exception e) {
			return new ReentrantReadWriteLock();
		}
	}

	private ReadWriteLock remove(Binary key, ReadWriteLock lock) {
		locks.computeIfPresent(key, (_key, old) -> {
			try {
				pool.returnObject(old);
			} catch (Exception e) {
			}
			return null;
		});
		return null;
	}

	public void unlock(byte[] key, Lock lock) {
		if (lock == null)
			return;
		lock.unlock();
		Binary _key = new Binary().data(key);
		counter.computeIfPresent(_key, (__key, old) -> {
			if (old.decrementAndGet() == 0) {
				locks.computeIfPresent(__key, this::remove);
				return null;
			}
			return old;
		});
	}
}
