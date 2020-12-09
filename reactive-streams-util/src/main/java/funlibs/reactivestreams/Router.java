package funlibs.reactivestreams;

import java.util.LinkedList;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitFailureHandler;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;

public class Router<T> implements Processor<T, T> {
	private static final Logger LOG = LoggerFactory.getLogger(Router.class);
	
	private final Many<T> router;
	private final Flux<T> flux;
	private final EmitFailureHandler failureHandler;
	private final LinkedList<Subscription> upstreams = new LinkedList<>();

	public static <T> Router<T> buffer(int bufferSize, BiFunction<String, String, Boolean> failureHandler) {
		Many<T> many = Sinks.many().multicast().onBackpressureBuffer(bufferSize, true);
		return new Router<>(many, failureHandler);
	}

	public static <T> Router<T> direct(BiFunction<String, String, Boolean> failureHandler) {
		Many<T> many = Sinks.many().multicast().directBestEffort();
		return new Router<>(many, failureHandler);
	}

	public Router(Many<T> router, BiFunction<String, String, Boolean> failureHandler) {
		this.router = router;
		this.flux = router.asFlux();
		this.failureHandler = new EmitFailureHandler() {
			@Override
			public boolean onEmitFailure(SignalType signalType, EmitResult emitResult) {
				return failureHandler.apply(signalType.toString(), emitResult.name());
			}
		};
	}

	@Override
	public void onSubscribe(Subscription s) {
		upstreams.add(s);
		s.request(1);
	}

	@Override
	public void onNext(T t) {
		router.emitNext(t, failureHandler);
		upstreams.forEach(s -> s.request(1));
	}

	@Override
	public void onError(Throwable t) {
		router.emitError(t, failureHandler);
	}

	@Override
	public void onComplete() {
		LOG.debug("onComplete called.");
	}

	public void stop() {
		upstreams.forEach(s -> s.cancel());
		router.emitComplete(failureHandler);
		LOG.debug("stop called.");
	}

	@Override
	public void subscribe(Subscriber<? super T> s) {
		flux.subscribe(s);
		LOG.debug("subscribe called.");
	}
	
	public void subscribe(Consumer<? super T> consumer) {
		flux.subscribe(consumer);
		LOG.debug("subscribe called.");
	}
	
	public void subscribe(Consumer<? super T> consumer, Consumer<Throwable> errorConsumer) {
		flux.subscribe(consumer, errorConsumer);
		LOG.debug("subscribe called.");
	}
	
	public void subscribe(Consumer<? super T> consumer, Consumer<Throwable> errorConsumer, Runnable completeConsumer) {
		flux.subscribe(consumer, errorConsumer, completeConsumer);
		LOG.debug("subscribe called.");
	}
}
