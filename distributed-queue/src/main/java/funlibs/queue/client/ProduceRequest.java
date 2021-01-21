package funlibs.queue.client;

import lombok.Getter;
import lombok.Setter;
import lombok.experimental.Accessors;

@Setter
@Getter
@Accessors(fluent = true)
public class ProduceRequest {
	private String topic;
	private int partition;
	private byte[] key;
	private byte[] value;
}
