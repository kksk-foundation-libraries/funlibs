package funlibs.event.processing.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.event.processing.model.EventLogKey;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.reactivestreams.Router;
import funlibs.serializer.ColferSerializer;
import reactor.core.publisher.Flux;

public abstract class LoggingProcessor extends MessageProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(LoggingProcessor.class);

	private final BinaryStore eventLog;
	private final ColferSerializer<EventLogKey> eventLogKeySerializer = ColferSerializer.of(EventLogKey.class);

	public LoggingProcessor(BinaryStore eventLog) {
		super(Router.direct(), Router.direct());
		this.eventLog = eventLog;
	}

	public final void start() {
		LOG.debug("starting..");
		Flux //
			.from(upstream) //
			.map(msg -> {
				LOG.debug("saving..");
				byte[] key = eventLogKeySerializer.serialize(new EventLogKey().withKey(msg.key()).withId(msg.id()));
				byte[] value = msg.value();
				Boolean existed = !eventLog.putIfAbsent(key, value);
				msg.put("existed", existed);
				LOG.debug("existed:{}", msg.<Boolean>get("existed"));
				return msg;
			}) //
			.filter(msg -> !msg.<Boolean>get("existed")) //
			.map(msg -> {
				LOG.debug("id set");
				msg.value(msg.id());
				return msg;
			}) //
			.subscribe(downstream) //
		;
		LOG.debug("started.");
	}

	public final void stop() {
		eventLog.close();
	}
}
