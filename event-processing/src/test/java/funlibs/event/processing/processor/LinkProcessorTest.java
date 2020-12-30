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

public class LinkProcessorTest {
	static final SimpleLogging ll = SimpleLogging.of() //
		.loggingGroup("funlibs.event.processing")
		.showDateTime() //
		.showShortLogName() //
		.showThreadName() //
		.dateTimeFormat("yyyy-MM-dd HH:mm:ss:SSS") //
		.loggingGroupLogLevel("DEBUG") //
		.initialize() //
	;
	private static final Logger LOG = LoggerFactory.getLogger(LinkProcessorTest.class);

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
		final Router<Message> linkReceiver = Router.direct();
		final Router<Message> sender = Router.direct();
		sender.subscribe(msg -> {
			LOG.debug("key:{}", Strings.toHex(msg.key()));
			LOG.debug("id:{}", Strings.toHex(msg.id()));
			LOG.debug("value:{}", Strings.toHex(msg.value()));
		});
		BinaryStore eventLog = new LocalBinaryStore("eventLog");
		BinaryStore highWaterMark = new LocalBinaryStore("highWaterMark");
		BinaryStore lowWaterMark = new LocalBinaryStore("lowWaterMark");
		BinaryStore eventLinkAsc = new LocalBinaryStore("eventLinkAsc");
		BinaryStore eventLinkDesc = new LocalBinaryStore("eventLinkDesc");
		LoggingProcessor loggingProcessor = new LoggingProcessor(eventLog);
		LinkProcessor linkProcessor = new LinkProcessor(highWaterMark, lowWaterMark, eventLinkAsc, eventLinkDesc);
		loggingProcessor.receiver(receiver);
		loggingProcessor.sender(linkReceiver);
		linkProcessor.receiver(linkReceiver);
		linkProcessor.sender(sender);
		LOG.debug(">>>");
		linkProcessor.start();
		loggingProcessor.start();
		blocker.blockSilent();
		LOG.debug("dump");
		((LocalBinaryStore)eventLog).dump();
		((LocalBinaryStore)highWaterMark).dump();
		((LocalBinaryStore)lowWaterMark).dump();
		((LocalBinaryStore)eventLinkAsc).dump();
		((LocalBinaryStore)eventLinkDesc).dump();
		LOG.debug("closing");
		loggingProcessor.stop();
		linkProcessor.stop();
	}

}
