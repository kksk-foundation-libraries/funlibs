package funlibs.event.processing.processor;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.binary.Strings;
import funlibs.concurrent.Blocker;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.event.processing.persist.LocalBinaryStore;
import funlibs.logging.SimpleLogging;
import funlibs.reactivestreams.Router;
import reactor.core.publisher.Flux;

public class SimpleOrderProcessorTest {
	static final SimpleLogging ll = SimpleLogging.of() //
		.loggingGroup("funlibs.event.processing").showDateTime() //
		.showShortLogName() //
		.showThreadName() //
		.dateTimeFormat("yyyy-MM-dd HH:mm:ss:SSS") //
		.loggingGroupLogLevel("DEBUG") //
		.initialize() //
	;
	private static final Logger LOG = LoggerFactory.getLogger(SimpleOrderProcessorTest.class);

	@Test
	public void test_001() {
		final Blocker blocker = new Blocker();
		final Flux<Message> receiver = Flux.range(1, 10).map(index -> {
			LOG.debug("index:{}", index);
			Message msg = new Message();
			msg.key(("key-" + index).getBytes());
			msg.id(("id-" + index).getBytes());
			msg.value(("value-" + index).getBytes());
			return msg;
		}).doOnTerminate(() -> {
			LOG.debug("completed.");
			blocker.unblock();
		});
		final Router<Message> orderReceiver = Router.direct();
		final Router<Message> sender = Router.direct();
		sender.subscribe(msg -> {
			LOG.debug("key:{}, id:{}, value:{}", Strings.toHex(msg.key()), Strings.toHex(msg.id()), Strings.toHex(msg.value()));
		});
		BinaryStore eventLog = new LocalBinaryStore("eventLog");
		BinaryStore highWaterMark = new LocalBinaryStore("highWaterMark");
		BinaryStore lowWaterMark = new LocalBinaryStore("lowWaterMark");
		BinaryStore eventLinkAsc = new LocalBinaryStore("eventLinkAsc");
		BinaryStore eventLinkDesc = new LocalBinaryStore("eventLinkDesc");
		BinaryStore inProgress = new LocalBinaryStore("inProgress");
		LoggingProcessor loggingProcessor = new LoggingProcessor(eventLog, highWaterMark, lowWaterMark, eventLinkAsc, eventLinkDesc);
		SimpleOrderProcessor orderProcessor = new SimpleOrderProcessor(eventLog, lowWaterMark, eventLinkAsc, eventLinkDesc, inProgress, true, //
			(k, v) -> {
				LOG.debug("processing... key:{}, value:{}", Strings.toHex(k), Strings.toHex(v));
				return true;
			} //
		);
		loggingProcessor.receiver(receiver);
		loggingProcessor.sender(orderReceiver);
		orderProcessor.receiver(orderReceiver);
		orderProcessor.sender(sender);
		LOG.debug(">>>");
		orderProcessor.start();
		loggingProcessor.start();
		blocker.blockSilent();
		LOG.debug("dump");
		((LocalBinaryStore) eventLog).dump();
		((LocalBinaryStore) highWaterMark).dump();
		((LocalBinaryStore) lowWaterMark).dump();
		((LocalBinaryStore) eventLinkAsc).dump();
		((LocalBinaryStore) eventLinkDesc).dump();
		((LocalBinaryStore) inProgress).dump();
		LOG.debug("closing");
		loggingProcessor.stop();
		orderProcessor.stop();
	}

	@Test
	public void test_002() {
		final Blocker blocker = new Blocker();
		final Flux<Message> receiver = Flux.range(1, 10).map(index -> {
			LOG.debug("index:{}", index);
			Message msg = new Message();
			msg.key(("key-1").getBytes());
			msg.id(("id-" + index).getBytes());
			msg.value(("value-" + index).getBytes());
			return msg;
		}).doOnTerminate(() -> {
			LOG.debug("completed.");
			blocker.unblock();
		});
		final Router<Message> orderReceiver = Router.direct();
		final Router<Message> sender = Router.direct();
		sender.subscribe(msg -> {
			LOG.debug("key:{}, id:{}, value:{}", Strings.toHex(msg.key()), Strings.toHex(msg.id()), Strings.toHex(msg.value()));
		});
		BinaryStore eventLog = new LocalBinaryStore("eventLog");
		BinaryStore highWaterMark = new LocalBinaryStore("highWaterMark");
		BinaryStore lowWaterMark = new LocalBinaryStore("lowWaterMark");
		BinaryStore eventLinkAsc = new LocalBinaryStore("eventLinkAsc");
		BinaryStore eventLinkDesc = new LocalBinaryStore("eventLinkDesc");
		BinaryStore inProgress = new LocalBinaryStore("inProgress");
		LoggingProcessor loggingProcessor = new LoggingProcessor(eventLog, highWaterMark, lowWaterMark, eventLinkAsc, eventLinkDesc);
		SimpleOrderProcessor orderProcessor = new SimpleOrderProcessor(eventLog, lowWaterMark, eventLinkAsc, eventLinkDesc, inProgress, true, //
			(k, v) -> {
				LOG.debug("processing... key:{}, value:{}", Strings.toHex(k), Strings.toHex(v));
				return true;
			} //
		);
		loggingProcessor.receiver(receiver);
		loggingProcessor.sender(orderReceiver);
		orderProcessor.receiver(orderReceiver);
		orderProcessor.sender(sender);
		LOG.debug(">>>");
		orderProcessor.start();
		loggingProcessor.start();
		blocker.blockSilent();
		LOG.debug("dump");
		((LocalBinaryStore) eventLog).dump();
		((LocalBinaryStore) highWaterMark).dump();
		((LocalBinaryStore) lowWaterMark).dump();
		((LocalBinaryStore) eventLinkAsc).dump();
		((LocalBinaryStore) eventLinkDesc).dump();
		((LocalBinaryStore) inProgress).dump();
		LOG.debug("closing");
		loggingProcessor.stop();
		orderProcessor.stop();
	}
}
