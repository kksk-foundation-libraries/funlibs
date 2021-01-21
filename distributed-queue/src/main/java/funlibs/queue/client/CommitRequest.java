package funlibs.queue.client;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.experimental.Accessors;

@Getter
@AllArgsConstructor
@Accessors(fluent = true)
public class CommitRequest {
	private final String topic;
	private final int partition;
	private final Long id;
}
