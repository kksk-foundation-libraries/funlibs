package funlibs.event.processing.processor;

import funlibs.event.processing.model.EventLogKey;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.reactivestreams.Router;
import funlibs.serializer.ColferSerializer;
import reactor.core.publisher.Flux;

public abstract class LoggingProcessor extends MessageProcessor {
	private final BinaryStore eventLog;
	private final ColferSerializer<EventLogKey> eventLogKeySerializer = ColferSerializer.of(EventLogKey.class);

	public LoggingProcessor(BinaryStore eventLog) {
		super(Router.direct(), Router.direct());
		this.eventLog = eventLog;
	}

	public final void start() {
		Flux //
			.from(upstream) //
			.map(msg -> {
				byte[] key = eventLogKeySerializer.serialize(new EventLogKey().withKey(msg.key()).withId(msg.id()));
				byte[] value = msg.value();
				boolean existed = !eventLog.putIfAbsent(key, value);
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

	public final void stop() {
		eventLog.close();
	}
}
