package funlibs.event.processing.flow;

import funlibs.concurrent.ConcurrentUtil;
import funlibs.event.processing.Message;
import funlibs.event.processing.processor.InvocationProcessor;
import funlibs.event.processing.processor.LoggingProcessor;
import funlibs.event.processing.processor.ResultCheckProcessor;
import funlibs.event.processing.processor.RetryProcessor;
import lombok.Setter;
import lombok.experimental.Accessors;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;

@Setter
@Accessors(fluent = true)
public class Flow {
	LoggingProcessor loggingProcessor;
	InvocationProcessor invocationProcessor;
	ResultCheckProcessor resultCheckProcessor;
	RetryProcessor retryProcessor;

	Flow() {
	}

	private Runnable onStart;
	private Runnable onStop;

	private final Many<Message> from = Sinks.many().unicast().onBackpressureBuffer();

	public void start() {
		if (onStart != null) {
			onStart.run();
		}
		ConnectableFlux<Message> flux = from.asFlux().publish();
		flux.subscribe(loggingProcessor);
		flux.connect();
	}

	public void fire(byte[] key, byte[] value) {
		Message msg = new Message().key(key).value(value);
		EmitResult res = from.tryEmitNext(msg);
		while (res == EmitResult.FAIL_OVERFLOW) {
			ConcurrentUtil.sleepSlient(1);
			res = from.tryEmitNext(msg);
		}
	}

	public void stop() {
		EmitResult res = from.tryEmitComplete();
		while (res == EmitResult.FAIL_OVERFLOW) {
			ConcurrentUtil.sleepSlient(1);
			res = from.tryEmitComplete();
		}
		if (onStop != null) {
			onStop.run();
		}
	}
}
