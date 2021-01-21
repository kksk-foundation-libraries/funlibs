package funlibs.event.processing.flow;

import java.util.function.BiFunction;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.concurrent.ConcurrentUtil;
import funlibs.concurrent.PartitionedLock;
import funlibs.event.processing.persist.LocalBinaryStore;
import funlibs.logging.SimpleLogging;

public class FlowTest {
	static final SimpleLogging ll = SimpleLogging.of() //
		//		.loggingGroup("funlibs.event.processing.processor.SimpleOrderProcessor","funlibs.event.processing.processor.LoggingProcessor") //
		//		.loggingGroup("funlibs.event.processing") //
		.loggingGroup("funlibs.event.processing.flow.FlowTest") //
		.showDateTime() //
		.showShortLogName() //
		.showThreadName() //
		.dateTimeFormat("yyyy-MM-dd HH:mm:ss.SSS") //
		.loggingGroupLogLevel("DEBUG") //
		.initialize() //
	;
	static final Logger LOG = LoggerFactory.getLogger(FlowTest.class);

	@Test
	public void test() {
		LocalBinaryStore eventLink = new LocalBinaryStore("eventLink");
		LocalBinaryStore inProgress = new LocalBinaryStore("inProgress");
		BiFunction<byte[], byte[], Boolean> eventProcessor = (k, v) -> {
			LOG.debug("key:{}, value:{}", new String(k), new String(v));
			return true;
		};
		FlowBuilder builder = FlowBuilder.of();
		PartitionedLock locks = new PartitionedLock(10241);
		builder.logging().locks(locks).eventLink(eventLink);
		builder.invocation().locks(locks).eventLink(eventLink).inProgress(inProgress).eventProcessor(eventProcessor).parallelism(10);
		builder.resultCheck().locks(locks).eventLink(eventLink).inProgress(inProgress).parallelism(4);
		builder.retry().interval(10L);
		Flow flow = builder.build();
		LOG.debug("starting..");
		flow.start();
		LOG.debug("started.");
		for (int i = 0; i < 100; i++) {
			int k = i % 10;
			flow.fire( //
				("key-" + k).getBytes(), //
				("value-" + i).getBytes() //
			) //
			;
		}
		ConcurrentUtil.sleepSlient(3000);
		LOG.debug("stopping..");
		flow.stop();
		LOG.debug("stopped.");
		//		fail("Not yet implemented");
	}

}
