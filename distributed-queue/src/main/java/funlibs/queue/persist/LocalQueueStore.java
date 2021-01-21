package funlibs.queue.persist;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import lombok.Data;
import lombok.experimental.Accessors;

public class LocalQueueStore implements QueueStore {
	private ConcurrentMap<Binary, byte[]> internal = new ConcurrentHashMap<>();

	@Override
	public byte[] get(byte[] key) {
		return internal.get(new Binary().data(key));
	}

	@Override
	public void put(byte[] key, byte[] value) {
		internal.put(new Binary().data(key), value);
	}

	@Override
	public void remove(byte[] key) {
		internal.remove(new Binary().data(key));
	}

	@Data
	@Accessors(fluent = true)
	private static class Binary {
		byte[] data;
	}
}
