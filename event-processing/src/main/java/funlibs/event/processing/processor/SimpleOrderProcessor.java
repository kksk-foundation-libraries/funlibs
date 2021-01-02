package funlibs.event.processing.processor;

import java.util.function.BiFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.event.processing.model.EventLinkKey;
import funlibs.event.processing.model.EventLogKey;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.reactivestreams.Router;
import funlibs.serializer.ColferDeserializer;
import funlibs.serializer.ColferSerializer;
import reactor.core.publisher.Flux;

public class SimpleOrderProcessor extends MessageProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(SimpleOrderProcessor.class);

	private static final byte[] BLANK = new byte[1];

	private final BinaryStore eventLog;
	private final BinaryStore lowWaterMark;
	private final BinaryStore eventLinkAsc;
	private final BinaryStore eventLinkDesc;
	private final BinaryStore inProgress;
	private final boolean sync;
	private final BiFunction<byte[], byte[], Boolean> eventProcessor;
	private final ColferSerializer<EventLogKey> eventLogKeySerializer = ColferSerializer.of(EventLogKey.class);
	private final ColferSerializer<EventLinkKey> eventLinkKeySerializer = ColferSerializer.of(EventLinkKey.class);
	private final ColferDeserializer<EventLinkKey> eventLinkKeyDeserializer = ColferDeserializer.of(EventLinkKey.class);

	public SimpleOrderProcessor(BinaryStore eventLog, BinaryStore lowWaterMark, BinaryStore eventLinkAsc, BinaryStore eventLinkDesc, BinaryStore inProgress, boolean sync, BiFunction<byte[], byte[], Boolean> eventProcessor) {
		super(Router.direct(), Router.direct(), Router.direct());
		this.eventLog = eventLog;
		this.lowWaterMark = lowWaterMark;
		this.eventLinkAsc = eventLinkAsc;
		this.eventLinkDesc = eventLinkDesc;
		this.inProgress = inProgress;
		this.sync = sync;
		this.eventProcessor = eventProcessor;
	}

	@Override
	protected final void onStart() {
		LOG.debug("onStart");
		Flux<Message> flux0 = Flux //
			.from(upstream) //
			.map(msg -> {
				byte[] previous = lowWaterMark.get(msg.key());
				if (previous == null) {
					previous = BLANK;
				}
				EventLinkKey eventLinkKeyPrevious = new EventLinkKey().withKey(msg.key()).withId(previous);
				byte[] current = eventLinkAsc.get(eventLinkKeySerializer.serialize(eventLinkKeyPrevious));
				EventLinkKey eventLinkKeyCurrent = eventLinkKeyDeserializer.deserialize(current);
				msg.id(eventLinkKeyCurrent.getId());
				msg.normal(inProgress.putIfAbsent(msg.key(), eventLinkKeyCurrent.getId()));
				return msg;
			}) //
			.map(msg -> {
				if (msg.normal()) {
					byte[] key = eventLogKeySerializer.serialize(new EventLogKey().withKey(msg.key()).withId(msg.id()));
					byte[] value = eventLog.get(key);
					boolean result = eventProcessor.apply(msg.key(), value);
					msg.put("eventLogKey", key);
					msg.normal(result);
				}
				return msg;
			}) //
		;
		Flux<Message> flux1 = flux0;
		if (sync) {
			flux1 = flux0 //
				.map(msg -> {
					if (msg.normal()) {
						eventLog.remove(msg.get("eventLogKey"));
						lowWaterMark.put(msg.key(), msg.id());
						inProgress.remove(msg.key());
						byte[] key = msg.key();
						byte[] id = msg.id();
						EventLinkKey eventLinkKey = new EventLinkKey().withKey(key).withId(id);
						byte[] curr = eventLinkKeySerializer.serialize(eventLinkKey);
						byte[] next = eventLinkAsc.get(curr);
						byte[] prev = eventLinkDesc.get(curr);
						if (prev != null && next != null) {
							eventLinkAsc.put(prev, next);
							eventLinkDesc.put(next, prev);
						} else if (prev != null && next == null) {
							eventLinkAsc.remove(prev);
						}
						eventLinkAsc.remove(curr);
						eventLinkDesc.remove(curr);
					}
					return msg;
				}) //
			;
		}
		flux1 //
			.filter(msg -> msg.normal()) //
			.subscribe(downstream) //
		;
		flux1 //
			.filter(msg -> !msg.normal()) //
			.subscribe(abnormalstream) //
		;
	}

	@Override
	protected final void onStop() {
		LOG.debug("onStop");
		eventLog.close();
		lowWaterMark.close();
		eventLinkAsc.close();
		eventLinkDesc.close();
		inProgress.close();
	}
}
