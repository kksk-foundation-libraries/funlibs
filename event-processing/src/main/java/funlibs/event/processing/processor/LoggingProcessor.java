package funlibs.event.processing.processor;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.event.processing.model.EventLinkKey;
import funlibs.event.processing.model.EventLogKey;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.reactivestreams.Router;
import funlibs.serializer.ColferSerializer;
import reactor.core.publisher.Flux;

public class LoggingProcessor extends MessageProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(LoggingProcessor.class);

	private static final byte[] BLANK = new byte[1];

	private final BinaryStore eventLog;
	private final BinaryStore highWaterMark;
	private final BinaryStore lowWaterMark;
	private final BinaryStore eventLinkAsc;
	private final BinaryStore eventLinkDesc;
	private final ColferSerializer<EventLogKey> eventLogKeySerializer = ColferSerializer.of(EventLogKey.class);
	private final ColferSerializer<EventLinkKey> eventLinkKeySerializer = ColferSerializer.of(EventLinkKey.class);

	public LoggingProcessor(BinaryStore eventLog, BinaryStore highWaterMark, BinaryStore lowWaterMark, BinaryStore eventLinkAsc, BinaryStore eventLinkDesc) {
		super(Router.direct(), Router.direct());
		this.eventLog = eventLog;
		this.highWaterMark = highWaterMark;
		this.lowWaterMark = lowWaterMark;
		this.eventLinkAsc = eventLinkAsc;
		this.eventLinkDesc = eventLinkDesc;
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
				byte[] previous = highWaterMark.get(msg.key());
				if (previous == null) {
					previous = BLANK;
				}
				byte[] eventLinkKey = eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(msg.id()));
				byte[] eventLinkKeyPrevious = eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(previous));
				eventLinkDesc.put(eventLinkKey, eventLinkKeyPrevious);
				eventLinkAsc.put(eventLinkKeyPrevious, eventLinkKey);
				highWaterMark.put(msg.key(), msg.id());
				return msg;
			}) //
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
		highWaterMark.close();
		lowWaterMark.close();
		eventLinkDesc.close();
		eventLinkAsc.close();
	}
}
