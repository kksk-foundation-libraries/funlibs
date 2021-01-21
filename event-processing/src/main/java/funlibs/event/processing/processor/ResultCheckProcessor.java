package funlibs.event.processing.processor;

import java.util.concurrent.locks.Lock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.binary.Strings;
import funlibs.concurrent.PartitionedLock;
import funlibs.event.processing.Message;
import funlibs.event.processing.persist.BinaryQueue;
import funlibs.event.processing.persist.BinaryStore;
import lombok.Setter;
import lombok.experimental.Accessors;

public class ResultCheckProcessor extends MessageProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(ResultCheckProcessor.class);

	private final PartitionedLock locks;
	private final BinaryStore eventLink;
	private final BinaryStore inProgress;

	public ResultCheckProcessor(PartitionedLock locks, BinaryStore eventLink, BinaryStore inProgress) {
		super();
		this.locks = locks;
		this.eventLink = eventLink;
		this.inProgress = inProgress;
	}

	public ResultCheckProcessor(int parallelism, PartitionedLock locks, BinaryStore eventLink, BinaryStore inProgress) {
		super(parallelism);
		this.locks = locks;
		this.eventLink = eventLink;
		this.inProgress = inProgress;
	}

	@Override
	protected Message process(Message msg) {
		msg.normal(true).retry(false);
		if (LOG.isDebugEnabled()) {
			LOG.debug("start check key:{}, from:{}", Strings.toHex(msg.key()), msg.from());
		}
		Lock lock = locks.get(msg.key());
		try {
			lock.lock();
			boolean containsKey = inProgress.containsKey(msg.key());
			if (containsKey) {
				BinaryQueue queue = new BinaryQueue(msg.key(), eventLink);
				int size = queue.size();
				if (size > 0) {
					if (msg.result()) {
						byte[] last = queue.getLast();
						byte[] first = queue.removeFirst();
						if (LOG.isDebugEnabled()) {
							LOG.debug("key:{}, value:{}, first:{}, last:{}", Strings.toHex(msg.key()), Strings.toHex((msg.value() == null) ? new byte[0] : msg.value()), new String(first), new String(last));
						}
					} else {
						msg.retry(true);
						msg.normal(false);
						if (LOG.isDebugEnabled()) {
							LOG.debug("retry key:{}", Strings.toHex(msg.key()));
						}
					}
				}
				inProgress.remove(msg.key());
			}
		} finally {
			lock.unlock();
		}
		msg.timestamp(System.currentTimeMillis()).from("check");
		return msg;
	}

	@Setter
	@Accessors(fluent = true)
	public static class ResultCheckProcessorBuilder {
		private int parallelism = -1;
		private PartitionedLock locks;
		private BinaryStore eventLink;
		private BinaryStore inProgress;

		public ResultCheckProcessor build() {
			if (locks == null) {
				throw new NullPointerException("argument 1:locks is required.");
			}
			if (eventLink == null) {
				throw new NullPointerException("argument 2:eventLink is required.");
			}
			if (inProgress == null) {
				throw new NullPointerException("argument 3:inProgress is required.");
			}
			if (parallelism != -1) {
				return new ResultCheckProcessor(parallelism, locks, eventLink, inProgress);
			} else {
				return new ResultCheckProcessor(locks, eventLink, inProgress);
			}
		}
	}
}
