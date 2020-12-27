package funlibs.kvs;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.cache.Cache;
import javax.cache.Cache.Entry;
import javax.cache.CacheManager;
import javax.cache.integration.CompletionListener;

public class BinaryCache {
	public static BinaryCache of(Cache<BinaryKey, byte[]> cache) {
		return new BinaryCache(cache);
	}

	private final Cache<BinaryKey, byte[]> delegated;

	private BinaryCache(Cache<BinaryKey, byte[]> cache) {
		this.delegated = cache;
	}

	public byte[] get(byte[] key) {
		return delegated.get(BinaryKey.of(key));
	}

	public Map<byte[], byte[]> getAll(Set<? extends byte[]> keys) {
		Set<BinaryKey> binKeys = keys.stream().map(bs -> BinaryKey.of(bs)).collect(Collectors.toSet());
		Map<BinaryKey, byte[]> map = delegated.getAll(binKeys);
		Map<byte[], byte[]> result = new HashMap<>();
		map.forEach((k, v) -> result.put(k.data(), v));
		return result;
	}

	public boolean containsKey(byte[] key) {
		return delegated.containsKey(BinaryKey.of(key));
	}

	public void loadAll(Set<? extends byte[]> keys, boolean replaceExistingValues, CompletionListener completionListener) {
		Set<BinaryKey> binKeys = keys.stream().map(bs -> BinaryKey.of(bs)).collect(Collectors.toSet());
		delegated.loadAll(binKeys, replaceExistingValues, completionListener);
	}

	public void put(byte[] key, byte[] value) {
		delegated.put(BinaryKey.of(key), value);
	}

	public byte[] getAndPut(byte[] key, byte[] value) {
		return delegated.getAndPut(BinaryKey.of(key), value);
	}

	public void putAll(Map<? extends byte[], ? extends byte[]> map) {
		Map<BinaryKey, byte[]> _map = new HashMap<>();
		map.forEach((k, v) -> _map.put(BinaryKey.of(k), v));
		delegated.putAll(_map);
	}

	public boolean putIfAbsent(byte[] key, byte[] value) {
		return delegated.putIfAbsent(BinaryKey.of(key), value);
	}

	public boolean remove(byte[] key) {
		return delegated.remove(BinaryKey.of(key));
	}

	public boolean remove(byte[] key, byte[] oldValue) {
		return delegated.remove(BinaryKey.of(key), oldValue);
	}

	public byte[] getAndRemove(byte[] key) {
		return delegated.getAndRemove(BinaryKey.of(key));
	}

	public boolean replace(byte[] key, byte[] oldValue, byte[] newValue) {
		return delegated.replace(BinaryKey.of(key), oldValue, newValue);
	}

	public boolean replace(byte[] key, byte[] value) {
		return delegated.replace(BinaryKey.of(key), value);
	}

	public byte[] getAndReplace(byte[] key, byte[] value) {
		return delegated.getAndReplace(BinaryKey.of(key), value);
	}

	public void removeAll(Set<? extends byte[]> keys) {
		Set<BinaryKey> binKeys = keys.stream().map(bs -> BinaryKey.of(bs)).collect(Collectors.toSet());
		delegated.removeAll(binKeys);
	}

	public void removeAll() {
		delegated.removeAll();
	}

	public void clear() {
		delegated.clear();
	}

	public String getName() {
		return delegated.getName();
	}

	public CacheManager getCacheManager() {
		return delegated.getCacheManager();
	}

	public void close() {
		delegated.close();
	}

	public boolean isClosed() {
		return delegated.isClosed();
	}

	public Iterator<Entry<byte[], byte[]>> iterator() {
		return new Iterator<Entry<byte[], byte[]>>() {
			final Iterator<Entry<BinaryKey, byte[]>> ite = delegated.iterator();

			@Override
			public boolean hasNext() {
				return ite.hasNext();
			}

			@Override
			public Entry<byte[], byte[]> next() {
				Entry<BinaryKey, byte[]> _entry = ite.next();
				if (_entry == null) {
					return null;
				}
				return new Entry<byte[], byte[]>() {
					final byte[] key = _entry.getKey().data();
					final byte[] value = _entry.getValue();

					@Override
					public <T> T unwrap(Class<T> clazz) {
						throw new UnsupportedOperationException();
					}

					@Override
					public byte[] getValue() {
						return value;
					}

					@Override
					public byte[] getKey() {
						return key;
					}
				};
			}
		};
	}
}
