package funlibs.event.processing.processor;

import funlibs.event.processing.model.EventLinkKey;
import funlibs.event.processing.model.EventLogKey;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.reactivestreams.Router;
import funlibs.serializer.ColferSerializer;
import reactor.core.publisher.Flux;

public class SimpleAsyncResultProcessor extends MessageProcessor {
	private final BinaryStore eventLog;
	private final BinaryStore lowWaterMark;
	private final BinaryStore eventLinkAsc;
	private final BinaryStore eventLinkDesc;
	private final BinaryStore inProgress;
	private final ColferSerializer<EventLogKey> eventLogKeySerializer = ColferSerializer.of(EventLogKey.class);
	private final ColferSerializer<EventLinkKey> eventLinkKeySerializer = ColferSerializer.of(EventLinkKey.class);

	public SimpleAsyncResultProcessor(BinaryStore eventLog, BinaryStore lowWaterMark, BinaryStore eventLinkAsc, BinaryStore eventLinkDesc, BinaryStore inProgress) {
		super(Router.direct(), Router.direct(), Router.direct());
		this.eventLog = eventLog;
		this.lowWaterMark = lowWaterMark;
		this.eventLinkAsc = eventLinkAsc;
		this.eventLinkDesc = eventLinkDesc;
		this.inProgress = inProgress;
	}

	@Override
	protected final void onStart() {
		Flux<Message> flux = Flux //
			.from(upstream) //
			.map(msg -> {
				boolean normal = !(msg.value() == null || msg.value().length == 0 || (msg.value()[0] & 0xff) == 1);
				msg.normal(normal);
				return msg;
			}) //
			.map(msg -> {
				if (msg.normal()) {
					byte[] previous = lowWaterMark.get(msg.key());
					byte[] current = eventLinkAsc.get(eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(previous)));
					byte[] eventLinkKey = eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(current));
					byte[] next = eventLinkAsc.getAndRemove(eventLinkKey);
					byte[] eventLinkKeyPrevious = eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(previous));
					if (next != null) {
						byte[] eventLinkKeyNext = eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(next));
						eventLinkDesc.put(eventLinkKeyNext, previous);
						eventLinkAsc.put(eventLinkKeyPrevious, next);
					} else {
						eventLinkAsc.remove(eventLinkKeyPrevious);
					}
					eventLinkDesc.remove(eventLinkKey);
					eventLog.remove(eventLogKeySerializer.serialize(new EventLogKey().withKey(msg.key()).withId(current)));
					lowWaterMark.put(msg.key(), msg.id());

				}
				return msg;
			}) //
		;
		flux //
			.filter(msg -> msg.normal()) //
			.subscribe(downstream) //
		;
		flux //
			.filter(msg -> !msg.normal()) //
			.subscribe(abnormalstream) //
		;
	}

	@Override
	protected final void onStop() {
		eventLog.close();
		lowWaterMark.close();
		eventLinkAsc.close();
		inProgress.close();
	}
}
