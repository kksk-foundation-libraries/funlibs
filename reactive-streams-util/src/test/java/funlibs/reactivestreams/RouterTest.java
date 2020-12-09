package funlibs.reactivestreams;

import static org.junit.Assert.*;

import java.util.LinkedList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;
import java.util.function.Consumer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.logging.SimpleLogging;
import reactor.core.publisher.Flux;

public class RouterTest {
	static SimpleLogging logconfing = SimpleLogging.forPackageDebug();
	
	static final Logger LOG = LoggerFactory.getLogger(RouterTest.class);

	@Test
	public void test001() {
		final AtomicReference<Consumer<Integer>> fluxSink1 = new AtomicReference<>();
		final AtomicReference<Consumer<Integer>> fluxSink2 = new AtomicReference<>();
		final CountDownLatch latch1_1 = new CountDownLatch(1);
		final CountDownLatch latch1_2 = new CountDownLatch(1);
		final CountDownLatch latch2_1 = new CountDownLatch(1);
		final CountDownLatch latch2_2 = new CountDownLatch(1);
		final LinkedList<String> result = new LinkedList<>();
		Flux<Integer> flux1 = Flux.create(sink -> {
			fluxSink1.set(t -> {
				sink.next(t);
				if (t.intValue() >= 9) {
					sink.complete();
				}
			});
			sink.onCancel(() -> fluxSink1.set(null));
			latch1_1.countDown();
		});
		Flux<Integer> flux2 = Flux.create(sink -> {
			fluxSink2.set(t -> {
				sink.next(t);
				if (t.intValue() >= 19) {
					sink.complete();
				}
			});
			sink.onCancel(() -> fluxSink2.set(null));
			latch1_2.countDown();
		});
		BiFunction<String, String, Boolean> failureHandler = (signalType, emitResult) -> false;
		Router<Integer> router1 = Router.direct(failureHandler);
		flux1.subscribe(router1);
		flux2.subscribe(router1);
		router1.subscribe(t -> result.add("data..(1):" + t), t -> {
		}, () -> latch2_1.countDown());
		try {
			latch1_1.await();
			latch1_2.await();
		} catch (InterruptedException e) {
		}
		int index = 0;
		while (index < 10 && fluxSink1.get() != null) {
			fluxSink1.get().accept(index);
			index++;
		}
		router1.subscribe(t -> result.add("data..(2):" + t), t -> {
		}, () -> latch2_2.countDown());
		while (index < 20 && fluxSink2.get() != null) {
			fluxSink2.get().accept(index);
			index++;
		}
		router1.stop();
		try {
			latch2_1.await();
			latch2_2.await();
		} catch (InterruptedException e) {
		}
		LOG.info("result:{}", result);
		assertTrue("processed will be 30", result.size() == 30);
		LOG.info("completed.");
	}

}
