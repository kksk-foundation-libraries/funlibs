package funlibs.event.processing.processor;

import org.junit.Test;
import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.binary.Strings;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.event.processing.persist.LocalBinaryStore;
import funlibs.logging.SimpleLogging;
import funlibs.reactivestreams.Router;
import reactor.core.publisher.Flux;

public class LoggingProcessorTest {
	static final SimpleLogging ll = SimpleLogging.forPackageDebug();
	private static final Logger LOG = LoggerFactory.getLogger(LoggingProcessorTest.class);

	@Test
	public void test_001() {
		final Flux<Message> receiver = Flux.range(1, 10).map(index -> {
			LOG.debug("index:{}", index);
			Message msg = new Message();
			msg.key(("key-" + index).getBytes());
			msg.id(("id-" + index).getBytes());
			msg.value(("value-" + index).getBytes());
			return msg;
		});
		final Router<Message> sender = Router.direct();
		BinaryStore eventLog = new LocalBinaryStore("eventLog");
		sender.subscribe(msg -> {
			LOG.debug("key:{}", Strings.toHex(msg.key()));
			LOG.debug("id:{}", Strings.toHex(msg.id()));
			LOG.debug("value:{}", Strings.toHex(msg.value()));
		});
		LoggingProcessor processor = new LoggingProcessorImpl(eventLog, receiver, sender);
		LOG.debug(">>>");
		processor._start();
		LOG.debug("closing");
		processor._stop();
	}

	static class LoggingProcessorImpl extends LoggingProcessor {
		private Publisher<Message> receiver;
		private Subscriber<Message> sender;

		public LoggingProcessorImpl(BinaryStore eventLog, Publisher<Message> receiver, Subscriber<Message> sender) {
			super(eventLog);
			this.receiver = receiver;
			this.sender = sender;
		}

		@Override
		protected Subscriber<Message> sender() {
			return sender;
		}

		@Override
		protected Publisher<Message> receiver() {
			return receiver;
		}
	}
}
