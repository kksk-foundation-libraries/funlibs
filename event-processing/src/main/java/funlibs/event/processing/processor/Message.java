package funlibs.event.processing.processor;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import org.reactivestreams.Publisher;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@Accessors(fluent = true)
public class Message {
	public static Message of() {
		return new Message();
	}

	private byte[] key;
	private byte[] id;
	private byte[] value;
	private boolean normal = true;
	private Callable<Publisher<Void>> commitable;
	private final Map<String, Object> meta = new HashMap<>();

	public <T> void put(String key, T value) {
		meta.put(key, value);
	}

	@SuppressWarnings("unchecked")
	public <T> T get(String key) {
		return (T) meta.get(key);
	}
}
