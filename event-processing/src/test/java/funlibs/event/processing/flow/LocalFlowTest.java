package funlibs.event.processing.flow;

import java.util.function.BiFunction;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.concurrent.ConcurrentUtil;
import funlibs.concurrent.PartitionedLock;
import funlibs.event.processing.Message;
import funlibs.event.processing.persist.LocalBinaryStore;
import funlibs.logging.SimpleLogging;
import reactor.core.publisher.Flux;

public class LocalFlowTest {
	static final SimpleLogging ll = SimpleLogging.of() //
		//		.loggingGroup("funlibs.event.processing.processor.SimpleOrderProcessor","funlibs.event.processing.processor.LoggingProcessor") //
//		.loggingGroup("funlibs.event.processing") //
		.loggingGroup("funlibs.event.processing.flow.LocalFlowTest") //
		.showDateTime() //
		.showShortLogName() //
		.showThreadName() //
		.dateTimeFormat("yyyy-MM-dd HH:mm:ss.SSS") //
		.loggingGroupLogLevel("DEBUG") //
		.initialize() //
	;
	static final Logger LOG = LoggerFactory.getLogger(LocalFlowTest.class);

	@Test
	public void test() {
		LocalBinaryStore eventLink = new LocalBinaryStore("eventLink");
		LocalBinaryStore inProgress = new LocalBinaryStore("inProgress");
		Publisher<Message> input = Flux.<Message>create(sink -> {
			for (int i = 0; i < 100; i++) {
				int k = i % 10;
				sink.next( //
					new Message() //
						.key(("key-" + k).getBytes()) //
						.value(("value-" + i).getBytes()) //
				) //
				;
			}
			sink.complete();
		}) //
		;
		BiFunction<byte[], byte[], Boolean> eventProcessor = (k, v) -> {
			LOG.debug("key:{}, value:{}", new String(k), new String(v));
			return true;
		};
		LocalFlow flow = new LocalFlow(new PartitionedLock(10241), eventLink, inProgress, input, eventProcessor, 10L);
		flow.processParallelism(4).checkParallelism(4).retryParallelism(2);
		//new LocalFlow(new PartitionedLock(10241), eventLink, inProgress, input, eventProcessor);
		flow.start();
		ConcurrentUtil.sleepSlient(3000);
		flow.stop();
		//		fail("Not yet implemented");
	}

}
