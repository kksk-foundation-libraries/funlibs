package funlibs.event.processing.flow;

import java.util.LinkedList;
import java.util.concurrent.locks.Lock;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.concurrent.ConcurrentUtil;
import funlibs.concurrent.PartitionedLock;
import funlibs.event.processing.Message;
import funlibs.event.processing.persist.BinaryQueue;
import funlibs.event.processing.persist.BinaryStore;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Sinks;
import reactor.core.publisher.Sinks.EmitResult;
import reactor.core.publisher.Sinks.Many;
import reactor.core.scheduler.Schedulers;

public class LocalFlow {
	private static final Logger LOG = LoggerFactory.getLogger(LocalFlow.class);
	private PartitionedLock locks;
	private BinaryStore eventLink;
	private BinaryStore inProgress;

	private Publisher<Message> input;

	private BiFunction<byte[], byte[], Boolean> eventProcessor;
	private long interval;

	private int processParallelism = 10;
	private int checkParallelism = 10;
	private int retryParallelism = 10;

	protected final Many<Message> loggingToProcess = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> processIn = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> processToCheck = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> processToError = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> processToRetry = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> checkIn = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> checkToError = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> checkToRetry = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> errorIn = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> retryIn = Sinks.many().unicast().onBackpressureBuffer();
	protected final Many<Message> retryToProcess = Sinks.many().unicast().onBackpressureBuffer();

	public LocalFlow(PartitionedLock locks, BinaryStore eventLink, BinaryStore inProgress, Publisher<Message> input, BiFunction<byte[], byte[], Boolean> eventProcessor, long interval) {
		this.locks = locks;
		this.eventLink = eventLink;
		this.inProgress = inProgress;
		this.input = input;
		this.eventProcessor = eventProcessor;
		this.interval = interval;
	}

	public LocalFlow processParallelism(int processParallelism) {
		this.processParallelism = processParallelism;
		return this;
	}

	public LocalFlow checkParallelism(int checkParallelism) {
		this.checkParallelism = checkParallelism;
		return this;
	}

	public LocalFlow retryParallelism(int retryParallelism) {
		this.retryParallelism = retryParallelism;
		return this;
	}

	public void start() {
		LinkedList<Runnable> inverseBatch = new LinkedList<>();
		inverseBatch.addFirst(this::startLogging);
		inverseBatch.addFirst(this::startLoggingToProcess);
		inverseBatch.addFirst(this::startProcess);
		inverseBatch.addFirst(this::startProcessToCheck);
		inverseBatch.addFirst(this::startProcessToError);
		inverseBatch.addFirst(this::startProcessToRetry);
		inverseBatch.addFirst(this::startError);
		inverseBatch.addFirst(this::startCheck);
		inverseBatch.addFirst(this::startCheckToError);
		inverseBatch.addFirst(this::startCheckToRetry);
		inverseBatch.addFirst(this::startRetry);
		inverseBatch.addFirst(this::startRetryToProcess);
		int jobs = inverseBatch.size();
		for (int i = 0; i < jobs; i++) {
			inverseBatch.removeFirst().run();
		}
	}

	private void completeSink(Many<Message> many) {
		EmitResult res = many.tryEmitComplete();
		while (emitRetry(res)) {
			ConcurrentUtil.sleepSlient(1);
			res = many.tryEmitComplete();
		}
	}

	private void onError(Throwable t) {
		stop();
	}

	public void stop() {
		LOG.debug("stop {}", "loggingToProcess");
		completeSink(loggingToProcess);
		LOG.debug("stop {}", "processIn");
		completeSink(processIn);
		LOG.debug("stop {}", "processToCheck");
		completeSink(processToCheck);
		LOG.debug("stop {}", "processToError");
		completeSink(processToError);
		LOG.debug("stop {}", "processToRetry");
		completeSink(processToRetry);
		LOG.debug("stop {}", "checkIn");
		completeSink(checkIn);
		LOG.debug("stop {}", "checkToError");
		completeSink(checkToError);
		LOG.debug("stop {}", "checkToRetry");
		completeSink(checkToRetry);
		LOG.debug("stop {}", "errorIn");
		completeSink(errorIn);
		LOG.debug("stop {}", "retryIn");
		completeSink(retryIn);
		LOG.debug("stop {}", "retryToProcess");
		completeSink(retryToProcess);
		LOG.debug("stop {}", "all");
	}

	public void startLogging() {
		Flux //
			.from(input) //
			.publishOn(Schedulers.newSingle("logging")) //
			.map(msg -> {
				msg.normal(true).result(true).retry(false);
				LOG.debug("start logging key:{}, value:{}", new String(msg.key()), new String(msg.value()));
				return msg;
			}) //
			.map(func(msg -> {
				Lock lock = locks.get(msg.key());
				try {
					lock.lock();
					BinaryQueue queue = new BinaryQueue(msg.key(), eventLink);
					queue.addLast(msg.value());
				} finally {
					lock.unlock();
				}
			})) //
			.map(msg -> {
				msg.timestamp(System.currentTimeMillis()).from("logging");
				return msg;
			}) //
			.subscribe(msg -> emitNext(loggingToProcess, msg)) //
		;
	}

	protected void startLoggingToProcess() {
		loggingToProcess //
			.asFlux() //
			.subscribe(msg -> emitNext(processIn, msg)) //
		;
	}

	private void startProcess() {
		processIn //
			.asFlux() //
			.parallel(processParallelism) //
			.runOn(Schedulers.newParallel("process", processParallelism)) //
			.map(msg -> {
				msg.normal(true).result(true).retry(false);
				LOG.debug("start process key:{}, from:{}", new String(msg.key()), msg.from());
				return msg;
			}) //
			.map(func(msg -> {
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
							LOG.debug("key:{}, size:{}, size2:{}", new String(msg.key()), size, eventLink.size());
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
			})) //
			.map(msg -> {
				msg.timestamp(System.currentTimeMillis()).from("order");
				return msg;
			}) //
			.doOnError(this::onError) //
			.sequential() //
			.subscribe(msg -> {
				if (msg.normal()) {
					emitNext(processToCheck, msg);
				} else {
					if (msg.retry()) {
						emitNext(processToRetry, msg);
					} else {
						emitNext(processToError, msg);
					}
				}
			}) //
		;
	}

	protected void startProcessToCheck() {
		processToCheck //
			.asFlux() //
			.subscribe(msg -> emitNext(checkIn, msg)) //
		;
	}

	protected void startProcessToError() {
		processToError //
			.asFlux() //
			.subscribe(msg -> emitNext(errorIn, msg)) //
		;
	}

	protected void startProcessToRetry() {
		processToRetry //
			.asFlux() //
			.subscribe(msg -> emitNext(retryIn, msg)) //
		;
	}

	private void startError() {
		errorIn //
			.asFlux() //
			.doOnError(this::onError) //
			.subscribe();
	}

	private void startCheck() {
		checkIn.asFlux() //
			.parallel(checkParallelism) //
			.runOn(Schedulers.newParallel("check", checkParallelism)) //
			.map(msg -> {
				msg.normal(true).retry(false);
				LOG.debug("start check key:{}, from:{}", new String(msg.key()), msg.from());
				return msg;
			}) //
			.map(func(msg -> {
				Lock lock = locks.get(msg.key());
				try {
					lock.lock();
					boolean result = inProgress.containsKey(msg.key());
					LOG.debug("check key:{}, containsKey:{}", new String(msg.key()), result);
					if (result) {
						BinaryQueue queue = new BinaryQueue(msg.key(), eventLink);
						int size = queue.size();
						if (size > 0) {
							if (msg.result()) {
								byte[] last = queue.getLast();
								byte[] first = queue.removeFirst();
								LOG.debug("key:{}, value:{}, first:{}, last:{}", new String(msg.key()), new String((msg.value() == null) ? new byte[0] : msg.value()), new String(first), new String(last));
							} else {
								msg.retry(true);
								msg.normal(false);
								LOG.debug("retry key:{}", new String(msg.key()));
							}
						}
						inProgress.remove(msg.key());
					}
				} finally {
					lock.unlock();
				}
			})) //
			.map(msg -> {
				msg.timestamp(System.currentTimeMillis()).from("check");
				return msg;
			}) //
			.doOnError(this::onError) //
			.sequential() //
			.subscribe(msg -> {
				if (!msg.normal()) {
					if (msg.retry()) {
						emitNext(checkToRetry, msg);
					} else {
						emitNext(checkToError, msg);
					}
				}
			});
	}

	protected void startCheckToError() {
		checkToError //
			.asFlux() //
			.subscribe(msg -> emitNext(errorIn, msg)) //
		;
	}

	protected void startCheckToRetry() {
		checkToRetry //
			.asFlux() //
			.subscribe(msg -> emitNext(retryIn, msg)) //
		;
	}

	private void startRetry() {
		retryIn //
			.asFlux() //
			.parallel(retryParallelism) //
			.runOn(Schedulers.newParallel("retry", retryParallelism)) //
			.map(msg -> {
				msg.normal(true).result(true).retry(false);
				LOG.debug("start retry key:{}, from:{}", new String(msg.key()), msg.from());
				return new Message().key(msg.key()).timestamp(msg.timestamp());
			}) //
			.map(func(msg -> {
				long millis = (msg.timestamp() + interval) - System.currentTimeMillis();
				if (millis > 0) {
					try {
						Thread.sleep(millis);
					} catch (InterruptedException e) {
					}
				}
			})) //
			.map(func(msg -> {
				msg.timestamp(System.currentTimeMillis()).from("retry");
			})) //
			.sequential() //
			.doOnError(this::onError) //
			.subscribe(msg -> {
				emitNext(retryToProcess, msg);
			});
	}

	protected void startRetryToProcess() {
		retryToProcess //
			.asFlux() //
			.subscribe(msg -> emitNext(processIn, msg)) //
		;
	}

	private Function<Message, Message> func(Consumer<Message> consumer) {
		return msg -> {
			if (msg.normal()) {
				consumer.accept(msg);
			}
			return msg;
		};
	}

	protected void emitNext(Many<Message> many, Message msg) {
		EmitResult res = many.tryEmitNext(msg);
		while (emitRetry(res)) {
			ConcurrentUtil.sleepSlient(1);
			res = many.tryEmitNext(msg);
		}
		if (res.isFailure()) {
			if (many == loggingToProcess) {
				LOG.debug("{} emit failure cause of {}", "sourceOut", res.name());
			} else if (many == processIn) {
				LOG.debug("{} emit failure cause of {}", "processIn", res.name());
			} else if (many == processToCheck) {
				LOG.debug("{} emit failure cause of {}", "processToCheck", res.name());
			} else if (many == processToError) {
				LOG.debug("{} emit failure cause of {}", "processToError", res.name());
			} else if (many == processToRetry) {
				LOG.debug("{} emit failure cause of {}", "processToRetry", res.name());
			} else if (many == checkIn) {
				LOG.debug("{} emit failure cause of {}", "check", res.name());
			} else if (many == checkToError) {
				LOG.debug("{} emit failure cause of {}", "checkToError", res.name());
			} else if (many == checkToRetry) {
				LOG.debug("{} emit failure cause of {}", "checkToRetry", res.name());
			} else if (many == errorIn) {
				LOG.debug("{} emit failure cause of {}", "errorIn", res.name());
			} else if (many == retryIn) {
				LOG.debug("{} emit failure cause of {}", "retryIn", res.name());
			} else if (many == retryToProcess) {
				LOG.debug("{} emit failure cause of {}", "retryToProcess", res.name());
			}
		}
	}

	private boolean emitRetry(EmitResult emitResult) {
		if (emitResult.isSuccess()) {
			return false;
		}
		if (!emitResult.isFailure()) {
			return true;
		}
		switch (emitResult) {
		case FAIL_OVERFLOW:
		case FAIL_NON_SERIALIZED:
			return true;
		default:
			return false;
		}
	}
}
