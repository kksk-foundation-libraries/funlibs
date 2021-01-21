package funlibs.event.processing.processor;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.reactivestreams.Processor;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import funlibs.event.processing.Message;
import reactor.core.Disposable;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;

public abstract class MessageProcessor implements Processor<Message, Message> {
	private ConnectableFlux<Message> flux;
	final AtomicReference<MessageSubscriber> ref = new AtomicReference<>();
	final AtomicReference<Disposable> disposableRef = new AtomicReference<>();
	private final List<Subscription> subscriptions = new ArrayList<>();

	public MessageProcessor() {
		this(1);
	}

	public MessageProcessor(int parallelism) {
		this(parallelism, null);
	}
	public MessageProcessor(Runnable onComplete) {
		this(1, onComplete);
	}
	public MessageProcessor(int parallelism, Runnable onComplete) {
		Flux<Message> _flux1 = //
			Flux //
				.<Message>create(sink -> {
					ref.set(new MessageSubscriber(msg -> sink.next(msg), t -> sink.error(t), onComplete == null ? () -> sink.complete() : onComplete));
					sink.onCancel(() -> {
						ref.set(null);
					});
				}) //
		;
		ParallelFlux<Message> _flux2 = null;
		if (parallelism > 1) {
			_flux2 = _flux1.parallel(parallelism).runOn(Schedulers.newParallel(getClass().getSimpleName()));
		} else {
			_flux2 = _flux1.parallel(1).runOn(Schedulers.newSingle(getClass().getSimpleName()));
		}
		Flux<Message> _flux3 = _flux2.map(this::process).sequential();
		flux = _flux3.publish();
	}

	protected abstract Message process(Message msg);

	public void start() {
		disposableRef.set(flux.connect());
	}

	public void stop() {
		if (disposableRef.get() != null) {
			disposableRef.get().dispose();
		}
	}

	@Override
	public void onSubscribe(Subscription s) {
		subscriptions.add(s);
		s.request(1);
	}

	@Override
	public void onNext(Message t) {
		if (ref.get() != null) {
			ref.get().onNext(t);
			subscriptions.forEach(s -> s.request(1));
		}
	}

	@Override
	public void onError(Throwable t) {
		if (ref.get() != null) {
			ref.get().onError(t);
		}
	}

	@Override
	public void onComplete() {
		if (ref.get() != null) {
			ref.get().onComplete();
		}
	}

	@Override
	public void subscribe(Subscriber<? super Message> s) {
		flux.subscribe(s);
	}

	private static class MessageSubscriber implements Subscriber<Message> {
		private final Consumer<Message> onNext;
		private final Consumer<Throwable> onError;
		private final Runnable onComplete;

		public MessageSubscriber(Consumer<Message> onNext, Consumer<Throwable> onError, Runnable onComplete) {
			this.onNext = onNext;
			this.onError = onError;
			this.onComplete = onComplete;
		}

		@Override
		public void onSubscribe(Subscription s) {
		}

		@Override
		public void onNext(Message t) {
			onNext.accept(t);
		}

		@Override
		public void onError(Throwable t) {
			onError.accept(t);
		}

		@Override
		public void onComplete() {
			onComplete.run();
		}

	}
}
