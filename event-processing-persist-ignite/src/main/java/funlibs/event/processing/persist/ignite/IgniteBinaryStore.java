package funlibs.event.processing.persist.ignite;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.cache.Cache.Entry;
import javax.cache.CacheManager;
import javax.cache.integration.CompletionListener;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteBinary;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.binary.BinaryObject;
import org.apache.ignite.binary.BinaryObjectBuilder;
import org.apache.ignite.cache.CachePeekMode;

import funlibs.event.processing.persist.BinaryStore;

public class IgniteBinaryStore implements BinaryStore {

	private final IgniteBinary binary;
	private final IgniteCache<BinaryObject, BinaryObject> cache;

	public IgniteBinaryStore(Ignite ignite, String name) {
		this.binary = ignite.binary();
		this.cache = ignite.getOrCreateCache(name).withKeepBinary();
	}

	public byte[] get(byte[] key) {
		return convert(cache.get(convert(key)));
	}

	private BinaryObject convert(byte[] data) {
		BinaryObjectBuilder builder = binary.builder("funlibs.ignite.BinaryObject");
		builder.setField("data", data, byte[].class);
		return builder.build();
	}

	private byte[] convert(BinaryObject binaryObject) {
		return binaryObject == null ? null : binaryObject.field("data");
	}

	public Map<byte[], byte[]> getAll(Set<? extends byte[]> keys) {
		Set<BinaryObject> binKeys = keys.stream().map(bs -> convert(bs)).collect(Collectors.toSet());
		Map<BinaryObject, BinaryObject> map = cache.getAll(binKeys);
		Map<byte[], byte[]> result = new HashMap<>();
		map.forEach((k, v) -> result.put(convert(k), convert(v)));
		return result;
	}

	public boolean containsKey(byte[] key) {
		return cache.containsKey(convert(key));
	}

	public void loadAll(Set<? extends byte[]> keys, boolean replaceExistingValues, CompletionListener completionListener) {
		Set<BinaryObject> binKeys = keys.stream().map(bs -> convert(bs)).collect(Collectors.toSet());
		cache.loadAll(binKeys, replaceExistingValues, completionListener);
	}

	public void put(byte[] key, byte[] value) {
		cache.put(convert(key), convert(value));
	}

	public byte[] getAndPut(byte[] key, byte[] value) {
		return convert(cache.getAndPut(convert(key), convert(value)));
	}

	public void putAll(Map<? extends byte[], ? extends byte[]> map) {
		Map<BinaryObject, BinaryObject> _map = new HashMap<>();
		map.forEach((k, v) -> _map.put(convert(k), convert(v)));
		cache.putAll(_map);
	}

	public boolean putIfAbsent(byte[] key, byte[] value) {
		return cache.putIfAbsent(convert(key), convert(value));
	}

	public boolean remove(byte[] key) {
		return cache.remove(convert(key));
	}

	public boolean remove(byte[] key, byte[] oldValue) {
		return cache.remove(convert(key), convert(oldValue));
	}

	public byte[] getAndRemove(byte[] key) {
		return convert(cache.getAndRemove(convert(key)));
	}

	public boolean replace(byte[] key, byte[] oldValue, byte[] newValue) {
		return cache.replace(convert(key), convert(oldValue), convert(newValue));
	}

	public boolean replace(byte[] key, byte[] value) {
		return cache.replace(convert(key), convert(value));
	}

	public byte[] getAndReplace(byte[] key, byte[] value) {
		return convert(cache.getAndReplace(convert(key), convert(value)));
	}

	public void removeAll(Set<? extends byte[]> keys) {
		Set<BinaryObject> binKeys = keys.stream().map(bs -> convert(bs)).collect(Collectors.toSet());
		cache.removeAll(binKeys);
	}

	public void removeAll() {
		cache.removeAll();
	}

	public void clear() {
		cache.clear();
	}

	public String getName() {
		return cache.getName();
	}

	public CacheManager getCacheManager() {
		return cache.getCacheManager();
	}

	public void close() {
		cache.close();
	}

	public boolean isClosed() {
		return cache.isClosed();
	}

	public Iterator<Entry<byte[], byte[]>> iterator() {
		return new Iterator<Entry<byte[], byte[]>>() {
			final Iterator<Entry<BinaryObject, BinaryObject>> ite = cache.iterator();

			public boolean hasNext() {
				return ite.hasNext();
			}

			public Entry<byte[], byte[]> next() {
				Entry<BinaryObject, BinaryObject> _entry = ite.next();
				if (_entry == null) {
					return null;
				}
				return new Entry<byte[], byte[]>() {
					final byte[] key = convert(_entry.getKey());
					final byte[] value = convert(_entry.getValue());

					public <T> T unwrap(Class<T> clazz) {
						throw new UnsupportedOperationException();
					}

					public byte[] getValue() {
						return value;
					}

					public byte[] getKey() {
						return key;
					}
				};
			}
		};
	}

	@Override
	public int size() {
		return cache.size(CachePeekMode.PRIMARY);
	}
}
