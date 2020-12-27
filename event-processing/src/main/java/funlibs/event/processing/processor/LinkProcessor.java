package funlibs.event.processing.processor;

import funlibs.binary.Bits;
import funlibs.event.processing.model.EventLinkKey;
import funlibs.event.processing.persist.BinaryStore;
import funlibs.reactivestreams.Router;
import funlibs.serializer.ColferSerializer;
import reactor.core.publisher.Flux;

public abstract class LinkProcessor extends MessageProcessor {
	private static final byte[] BLANK = new byte[8];
	private final BinaryStore highWaterMark;
	private final BinaryStore lowWaterMark;
	private final BinaryStore eventLinkAsc;
	private final BinaryStore eventLinkDesc;
	private final ColferSerializer<EventLinkKey> eventLinkKeySerializer = ColferSerializer.of(EventLinkKey.class);

	public LinkProcessor(BinaryStore highWaterMark, BinaryStore lowWaterMark, BinaryStore eventLinkAsc, BinaryStore eventLinkDesc) {
		super(Router.direct(), Router.direct());
		this.highWaterMark = highWaterMark;
		this.lowWaterMark = lowWaterMark;
		this.eventLinkAsc = eventLinkAsc;
		this.eventLinkDesc = eventLinkDesc;
	}

	@Override
	protected final void start() {
		Flux //
			.from(upstream) //
			.map(msg -> {
				byte[] previous = highWaterMark.get(msg.key());
				if (previous == null) {
					previous = BLANK;
				}
				byte[] eventLinkKey = eventLinkKeySerializer.serialize(new EventLinkKey().withKey(msg.key()).withId(msg.value()));
				eventLinkDesc.put(eventLinkKey, previous);
				eventLinkAsc.put(previous, eventLinkKey);
				lowWaterMark.putIfAbsent(msg.key(), BLANK);
				highWaterMark.put(msg.key(), msg.value());
				return msg;
			}) //
			.map(msg -> {
				byte[] ts = new byte[Long.BYTES];
				Bits.putLong(ts, 0, System.currentTimeMillis());
				msg.value(ts);
				return msg;
			}) //
			.subscribe(downstream) //
		;
	}

	@Override
	protected final void stop() {
		highWaterMark.close();
		lowWaterMark.close();
		eventLinkAsc.close();
		eventLinkDesc.close();
	}

}
