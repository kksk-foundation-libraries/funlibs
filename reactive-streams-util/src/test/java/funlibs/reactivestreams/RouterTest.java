package funlibs.reactivestreams;

import static org.junit.Assert.assertTrue;

import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.concurrent.Blocker;
import funlibs.logging.SimpleLogging;
import reactor.core.publisher.Flux;

public class RouterTest {
	static SimpleLogging logconfing = SimpleLogging.forPackageDebug();
	
	static final Logger LOG = LoggerFactory.getLogger(RouterTest.class);

	@Test
	public void test001() {
		final AtomicReference<Consumer<Integer>> fluxSink1 = new AtomicReference<>();
		final AtomicReference<Consumer<Integer>> fluxSink2 = new AtomicReference<>();
		final Blocker blocker_1_1 = new Blocker();
		final Blocker blocker_1_2 = new Blocker();
		final Blocker blocker_2_1 = new Blocker();
		final Blocker blocker_2_2 = new Blocker();
		final LinkedList<String> result = new LinkedList<>();
		Flux<Integer> flux1 = Flux.create(sink -> {
			fluxSink1.set(t -> {
				sink.next(t);
				if (t.intValue() >= 9) {
					sink.complete();
				}
			});
			sink.onCancel(() -> fluxSink1.set(null));
			blocker_1_1.unblock();
		});
		Flux<Integer> flux2 = Flux.create(sink -> {
			fluxSink2.set(t -> {
				sink.next(t);
				if (t.intValue() >= 19) {
					sink.complete();
				}
			});
			sink.onCancel(() -> fluxSink2.set(null));
			blocker_1_2.unblock();
		});
		Router<Integer> router1 = Router.direct();
		flux1.subscribe(router1);
		flux2.subscribe(router1);
		router1.subscribe(t -> result.add("data..(1):" + t), t -> {
		}, () -> blocker_2_1.unblock());
		blocker_1_1.blockSilent();
		blocker_1_2.blockSilent();
		int index = 0;
		while (index < 10 && fluxSink1.get() != null) {
			fluxSink1.get().accept(index);
			index++;
		}
		router1.subscribe(t -> result.add("data..(2):" + t), t -> {
		}, () -> blocker_2_2.unblock());
		while (index < 20 && fluxSink2.get() != null) {
			fluxSink2.get().accept(index);
			index++;
		}
		router1.stop();
		blocker_2_1.blockSilent();
		blocker_2_2.blockSilent();
		LOG.info("result:{}", result);
		assertTrue("processed will be 30", result.size() == 30);
		LOG.info("completed.");
	}

}
