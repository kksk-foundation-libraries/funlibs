package funlibs.event.processing.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.binary.Strings;
import funlibs.event.processing.Message;
import lombok.Setter;
import lombok.experimental.Accessors;

public class RetryProcessor extends MessageProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(RetryProcessor.class);

	private final long interval;

	public RetryProcessor(long interval) {
		super();
		this.interval = interval;
	}

	public RetryProcessor(int parallelism, long interval) {
		super(parallelism);
		this.interval = interval;
	}

	@Override
	protected Message process(Message msg) {
		msg.normal(true).result(true).retry(false);
		if (LOG.isDebugEnabled()) {
			LOG.debug("start retry key:{}", Strings.toHex(msg.key()));
		}
		long millis = (msg.timestamp() + interval) - System.currentTimeMillis();
		if (millis > 0) {
			try {
				Thread.sleep(millis);
			} catch (InterruptedException e) {
			}
		}
		msg.timestamp(System.currentTimeMillis()).from("retry");
		return msg;
	}

	@Setter
	@Accessors(fluent = true)
	public static class RetryProcessorBuilder {
		private int parallelism = -1;
		private long interval = -1L;

		public RetryProcessor build() {
			if (interval < 0L) {
				throw new IllegalArgumentException("argument 1:interval is required.");
			}
			if (parallelism != -1) {
				return new RetryProcessor(parallelism, interval);
			} else {
				return new RetryProcessor(interval);
			}
		}
	}
}
