package funlibs.concurrent;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.ObjectPool;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.PooledObjectFactory;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;

import lombok.Data;
import lombok.experimental.Accessors;

public class KeyedLock {
	private final PooledObjectFactory<Lock> factory = new BasePooledObjectFactory<Lock>() {
		@Override
		public Lock create() throws Exception {
			return new ReentrantLock();
		}

		@Override
		public PooledObject<Lock> wrap(Lock value) {
			return new DefaultPooledObject<>(value);
		}
	};
	private final ObjectPool<Lock> pool;

	private final ConcurrentMap<Binary, AtomicInteger> counter = new ConcurrentHashMap<>();
	private final ConcurrentMap<Binary, Lock> locks = new ConcurrentHashMap<>();

	@Data
	@Accessors(fluent = true)
	private static class Binary {
		byte[] data;
	}

	public KeyedLock() {
		this(1024, 512, 1024);
	}

	public KeyedLock(int maxTotal, int minIdle, int maxIdle) {
		GenericObjectPoolConfig<Lock> cfg = new GenericObjectPoolConfig<>();
		cfg.setMaxIdle(maxIdle);
		cfg.setMinIdle(minIdle);
		cfg.setMaxTotal(maxTotal);
		pool = new GenericObjectPool<>(factory, cfg);
	}

	public Lock lock(byte[] key) {
		return locks.computeIfAbsent(new Binary().data(key), this::getOrCreate);
	}

	private Lock getOrCreate(Binary key) {
		counter.computeIfAbsent(key, _key -> new AtomicInteger()).incrementAndGet();
		try {
			return pool.borrowObject();
		} catch (Exception e) {
			return new ReentrantLock();
		}
	}

	private Lock remove(Binary key, Lock lock) {
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
