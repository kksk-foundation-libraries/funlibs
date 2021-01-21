package funlibs.queue.persist;

public interface QueueStore {
	byte[] get(byte[] key);

	void put(byte[] key, byte[] value);

	void remove(byte[] key);
}
