package funlibs.event.processing.processor;

import java.util.function.BiFunction;

import funlibs.event.processing.model.EventLinkKey;
import funlibs.event.processing.model.EventLogKey;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.reactivestreams.Router;
import funlibs.serializer.ColferSerializer;
import reactor.core.publisher.Flux;

public abstract class SimpleOrderProcessor extends MessageProcessor {
	private static final byte[] BLANK = new byte[8];
	private final BinaryStore eventLog;
	private final BinaryStore lowWaterMark;
	private final BinaryStore eventLinkAsc;
	private final BinaryStore eventLinkDesc;
	private final BinaryStore inProgress;
	private final boolean sync;
	private final BiFunction<byte[], byte[], Boolean> eventProcessor;
	private final ColferSerializer<EventLogKey> eventLogKeySerializer = ColferSerializer.of(EventLogKey.class);
	private final ColferSerializer<EventLinkKey> eventLinkKeySerializer = ColferSerializer.of(EventLinkKey.class);

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
	protected final void start() {
		Flux<Message> flux0 = Flux //
			.from(upstream) //
			.map(msg -> {
				msg.normal(inProgress.putIfAbsent(msg.key(), BLANK));
				return msg;
			}) //
			.map(msg -> {
				if (msg.normal()) {
					byte[] previous = lowWaterMark.get(msg.key());
					byte[] current = eventLinkAsc.get(eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(previous)));
					inProgress.put(msg.key(), current);
					msg.id(current);
					msg.put("previous", previous);
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
						byte[] eventLinkKey = eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(msg.id()));
						byte[] next = eventLinkAsc.getAndRemove(eventLinkKey);
						byte[] previous = msg.get("previous");
						byte[] eventLinkKeyPrevious = eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(previous));
						if (next != null) {
							byte[] eventLinkKeyNext = eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(next));
							eventLinkDesc.put(eventLinkKeyNext, previous);
							eventLinkAsc.put(eventLinkKeyPrevious, next);
						} else {
							eventLinkAsc.remove(eventLinkKeyPrevious);
						}
						eventLinkDesc.remove(eventLinkKey);
						eventLog.remove(msg.get("eventLogKey"));
						lowWaterMark.put(msg.key(), msg.id());

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
	protected final void stop() {
		eventLog.close();
		lowWaterMark.close();
		eventLinkAsc.close();
		inProgress.close();
	}
}
