package funlibs.event.processing.processor;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import funlibs.reactivestreams.Router;
import reactor.core.publisher.Flux;

public abstract class MessageProcessor {
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
		Flux //
			.from(receiver()) //
			.subscribe(upstream);
		;
		Flux //
			.from(downstream) //
			.subscribe(sender()) //
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
		stop();
	}

	protected abstract Publisher<Message> receiver();

	protected abstract Subscriber<Message> sender();

	protected Subscriber<Message> abnormalSender() {
		return null;
	}
}
