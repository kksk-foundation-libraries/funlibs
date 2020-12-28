package funlibs.event.processing.processor;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import funlibs.reactivestreams.Router;
import reactor.core.publisher.Flux;

public abstract class MessageProcessor {
	private static final Logger LOG = LoggerFactory.getLogger(MessageProcessor.class);
	final Router<Message> upstream;
	final Router<Message> downstream;
	final Router<Message> abnormalstream;

	public MessageProcessor(Router<Message> upstream, Router<Message> downstream) {
		this(upstream, downstream, null);
	}

	public MessageProcessor(Router<Message> upstream, Router<Message> downstream, Router<Message> abnormalstream) {
		this.upstream = upstream;
		this.downstream = downstream;
		this.abnormalstream = abnormalstream;
	}

	protected abstract void start();

	protected abstract void stop();

	public void _start() {
		start();
		Publisher<Message> receiver = receiver();
		Subscriber<Message> sender = sender();
		Flux //
		.from(downstream) //
		.subscribe(sender) //
		;
		Flux //
			.from(receiver) //
			.subscribe(upstream);
		;
		Subscriber<Message> abnormalSender = abnormalSender();
		if (abnormalSender != null && abnormalstream != null) {
			Flux //
				.from(abnormalstream) //
				.subscribe(abnormalSender) //
			;
		} else if (abnormalSender != null || abnormalstream != null) {
			// FIXME
		}
	}

	public void _stop() {
		upstream.stop();
		downstream.stop();
		if (abnormalstream != null) {
			abnormalstream.stop();
		}
		stop();
	}

	protected abstract Publisher<Message> receiver();

	protected abstract Subscriber<Message> sender();

	protected Subscriber<Message> abnormalSender() {
		return null;
	}
}
