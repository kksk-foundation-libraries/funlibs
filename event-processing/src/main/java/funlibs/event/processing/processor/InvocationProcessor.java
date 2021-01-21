package funlibs.event.processing.processor;

import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.binary.Strings;
import funlibs.concurrent.PartitionedLock;
import funlibs.event.processing.Message;
import funlibs.event.processing.persist.BinaryQueue;
import funlibs.event.processing.persist.BinaryStore;
import lombok.Setter;
import lombok.experimental.Accessors;

public class InvocationProcessor extends MessageProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(InvocationProcessor.class);

	private final PartitionedLock locks;
	private final BinaryStore eventLink;
	private final BinaryStore inProgress;
	private final BiFunction<byte[], byte[], Boolean> eventProcessor;

	public InvocationProcessor(PartitionedLock locks, BinaryStore eventLink, BinaryStore inProgress, BiFunction<byte[], byte[], Boolean> eventProcessor) {
		super(InvocationProcessor::doNothing);
		this.locks = locks;
		this.eventLink = eventLink;
		this.inProgress = inProgress;
		this.eventProcessor = eventProcessor;
	}

	public InvocationProcessor(int parallelism, PartitionedLock locks, BinaryStore eventLink, BinaryStore inProgress, BiFunction<byte[], byte[], Boolean> eventProcessor) {
		super(parallelism, InvocationProcessor::doNothing);
		this.locks = locks;
		this.eventLink = eventLink;
		this.inProgress = inProgress;
		this.eventProcessor = eventProcessor;
	}

	private static void doNothing() {
	}

	@Override
	protected Message process(Message msg) {
		msg.normal(true).result(true).retry(false);
		if (LOG.isDebugEnabled()) {
			LOG.debug("start invocation key:{}, from:{}", Strings.toHex(msg.key()), msg.from());
		}
		Lock lock = locks.get(msg.key());
		if (!lock.tryLock()) {
			msg.retry(true);
			msg.normal(false);
		} else {
			try {
				boolean result = inProgress.putIfAbsent(msg.key(), msg.key());
				if (!result) {
					msg.retry(true);
					msg.normal(false);
				} else {
					BinaryQueue queue = new BinaryQueue(msg.key(), eventLink);
					int size = queue.size();
					if (LOG.isDebugEnabled()) {
						LOG.debug("key:{}, size:{}, size2:{}", Strings.toHex(msg.key()), size, eventLink.size());
					}
					if (size > 0) {
						msg.value(queue.getFirst());
						result = eventProcessor.apply(msg.key(), msg.value());
						msg.result(result);
					}
				}
			} finally {
				lock.unlock();
			}
		}
		msg.timestamp(System.currentTimeMillis()).from("invocation");
		return msg;
	}

	@Setter
	@Accessors(fluent = true)
	public static class InvocationProcessorBuilder {
		private int parallelism = -1;
		private PartitionedLock locks;
		private BinaryStore eventLink;
		private BinaryStore inProgress;
		private BiFunction<byte[], byte[], Boolean> eventProcessor;

		public InvocationProcessor build() {
			if (locks == null) {
				throw new NullPointerException("argument 1:locks is required.");
			}
			if (eventLink == null) {
				throw new NullPointerException("argument 2:eventLink is required.");
			}
			if (inProgress == null) {
				throw new NullPointerException("argument 3:inProgress is required.");
			}
			if (eventProcessor == null) {
				throw new NullPointerException("argument 4:eventProcessor is required.");
			}
			if (parallelism > 0) {
				return new InvocationProcessor(parallelism, locks, eventLink, inProgress, eventProcessor);
			} else {
				return new InvocationProcessor(locks, eventLink, inProgress, eventProcessor);
			}
		}
	}
}
