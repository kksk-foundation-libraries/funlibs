package funlibs.kvs;

import javax.cache.Cache;

public class Sequencer {
	private final Cache<BinaryKey, Long> delegated;

	public Sequencer(Cache<BinaryKey, Long> delegated) {
		this.delegated = delegated;
	}

	public long nextVal(byte[] key) {
		return delegated.invoke(BinaryKey.of(key), (entry, obj) -> {
			if (entry.exists()) {
				entry.setValue(entry.getValue() + 1);
			} else {
				entry.setValue(1L);
			}
			return entry.getValue();
		});
	}
	
	public long currentVal(byte[] key) {
		return delegated.get(BinaryKey.of(key));
	}
}
