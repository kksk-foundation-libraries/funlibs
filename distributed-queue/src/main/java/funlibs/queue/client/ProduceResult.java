package funlibs.queue.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@AllArgsConstructor
@Accessors(fluent = true)
public class ProduceResult {
	private final Producer producer;
	private final String topic;
	private final int partition;
	private final Long id;

	public CommitRequest commit() {
		return new CommitRequest(topic, partition, id);
	}
}
