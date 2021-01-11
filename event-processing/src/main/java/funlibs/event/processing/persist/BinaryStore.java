package funlibs.event.processing.persist;

import java.util.Map;
import java.util.Set;

public interface BinaryStore {
	byte[] get(byte[] key);

	Map<byte[], byte[]> getAll(Set<? extends byte[]> keys);

	boolean containsKey(byte[] key);

	void put(byte[] key, byte[] value);

	byte[] getAndPut(byte[] key, byte[] value);

	void putAll(Map<? extends byte[], ? extends byte[]> map);

	boolean putIfAbsent(byte[] key, byte[] value);

	boolean remove(byte[] key);

	boolean remove(byte[] key, byte[] oldValue);

	byte[] getAndRemove(byte[] key);

	boolean replace(byte[] key, byte[] oldValue, byte[] newValue);

	boolean replace(byte[] key, byte[] value);

	byte[] getAndReplace(byte[] key, byte[] value);

	void removeAll(Set<? extends byte[]> keys);

	void removeAll();

	void clear();

	String getName();

	void close();

	boolean isClosed();

	int size();
}