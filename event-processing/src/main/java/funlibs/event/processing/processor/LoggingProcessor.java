package funlibs.event.processing.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.event.processing.model.EventLogKey;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.reactivestreams.Router;
import funlibs.serializer.ColferSerializer;
import reactor.core.publisher.Flux;

public class LoggingProcessor extends MessageProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(LoggingProcessor.class);

	private final BinaryStore eventLog;
	private final ColferSerializer<EventLogKey> eventLogKeySerializer = ColferSerializer.of(EventLogKey.class);

	public LoggingProcessor(BinaryStore eventLog) {
		super(Router.direct(), Router.direct());
		this.eventLog = eventLog;
	}

	public final void onStart() {
		LOG.debug("onStart");
		Flux //
			.from(upstream) //
			.map(msg -> {
				byte[] key = eventLogKeySerializer.serialize(new EventLogKey().withKey(msg.key()).withId(msg.id()));
				byte[] value = msg.value();
				Boolean existed = !eventLog.putIfAbsent(key, value);
				msg.put("existed", existed);
				return msg;
			}) //
			.filter(msg -> !msg.<Boolean>get("existed")) //
			.map(msg -> {
				msg.value(msg.id());
				return msg;
			}) //
			.subscribe(downstream) //
		;
	}

	public final void onStop() {
		LOG.debug("onStop");
		eventLog.close();
	}
}
