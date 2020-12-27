package funlibs.event.processing.processor;

import funlibs.binary.Bits;
import funlibs.reactivestreams.Router;
import reactor.core.publisher.Flux;

public abstract class RetryProcessor extends MessageProcessor {
	private final long interval;

	public RetryProcessor(long interval) {
		super(Router.direct(), Router.direct());
		this.interval = interval;
	}

	@Override
	protected final void start() {
		Flux //
			.from(upstream) //
			.map(msg -> {
				long ts = Bits.getLong(msg.value(), 0);
				long millis = System.currentTimeMillis() - ts - interval;
				if (millis > 0) {
					try {
						Thread.sleep(millis);
					} catch (InterruptedException e) {
					}
				}
				return msg;
			}) //
			.map(msg -> {
				byte[] ts = new byte[Long.BYTES];
				Bits.putLong(ts, 0, System.currentTimeMillis());
				msg.value(ts);
				return msg;
			}) //
			.subscribe(downstream) //
		;
	}

	@Override
	protected final void stop() {
	}

}
