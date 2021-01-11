package funlibs.event.processing;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@Accessors(fluent = true)
public class Message {
	private byte[] key;
	private byte[] value;
	private long timestamp;

	private String from;
	private boolean normal = true;
	private boolean retry = false;
	private boolean result = true;
}
