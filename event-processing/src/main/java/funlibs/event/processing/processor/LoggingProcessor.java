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

public class LoggingProcessor extends MessageProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(LoggingProcessor.class);
	private final PartitionedLock locks;
	private final BinaryStore eventLink;

	public LoggingProcessor(PartitionedLock locks, BinaryStore eventLink) {
		super();
		this.locks = locks;
		this.eventLink = eventLink;
	}

	@Override
	protected Message process(Message msg) {
		msg.normal(true).result(true).retry(false);
		if (LOG.isDebugEnabled()) {
			LOG.debug("start logging key:{}, value:{}", Strings.toHex(msg.key()), new String(msg.value()));
		}
		Lock lock = locks.get(msg.key());
		try {
			lock.lock();
			BinaryQueue queue = new BinaryQueue(msg.key(), eventLink);
			queue.addLast(msg.value());
		} finally {
			lock.unlock();
		}
		msg.timestamp(System.currentTimeMillis()).from("logging");
		return msg;
	}

	@Setter
	@Accessors(fluent = true)
	public static class LoggingProcessorBuilder {
		private PartitionedLock locks;
		private BinaryStore eventLink;

		public LoggingProcessor build() {
			if (locks == null) {
				throw new NullPointerException("argument 1:locks is required.");
			}
			if (eventLink == null) {
				throw new NullPointerException("argument 2:eventLink is required.");
			}
			return new LoggingProcessor(locks, eventLink);
		}
	}
}
