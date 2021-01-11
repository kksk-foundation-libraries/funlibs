package funlibs.reactivestreams;

import java.util.LinkedList;
import java.util.function.Consumer;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;

public class Router<T> implements Processor<T, T> {
	private static final Logger LOG = LoggerFactory.getLogger(Router.class);

	private final Many<T> router;
	private final Flux<T> flux;
	private final LinkedList<Subscription> upstreams = new LinkedList<>();

	public static <T> Router<T> buffer(int bufferSize) {
		Many<T> many = Sinks.many().multicast().onBackpressureBuffer(bufferSize);
		return new Router<>(many);
	}

	public static <T> Router<T> direct() {
		Many<T> many = Sinks.many().multicast().directBestEffort();
		return new Router<>(many);
	}

	public Router(Many<T> router) {
		this.router = router;
		this.flux = router.asFlux();
	}

	@Override
	public void onSubscribe(Subscription s) {
		upstreams.add(s);
		s.request(1);
	}

	@Override
	public void onNext(T t) {
		EmitResult res;
		res = router.tryEmitNext(t);
		while (!res.isSuccess()) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
			res = router.tryEmitNext(t);
		}
		upstreams.forEach(s -> s.request(1));
	}

	@Override
	public void onError(Throwable t) {
		EmitResult res;
		res = router.tryEmitError(t);
		while (!res.isSuccess()) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
			res = router.tryEmitError(t);
		}
	}

	@Override
	public void onComplete() {
		LOG.debug("onComplete called.");
	}

	public void stop() {
		upstreams.forEach(s -> s.cancel());
		EmitResult res;
		res = router.tryEmitComplete();
		while (!res.isSuccess()) {
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
			}
			res = router.tryEmitComplete();
		}
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
