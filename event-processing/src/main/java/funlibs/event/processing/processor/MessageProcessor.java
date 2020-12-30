package funlibs.event.processing.processor;

import org.reactivestreams.Publisher;
import org.reactivestreams.Subscriber;

import funlibs.reactivestreams.Router;
import reactor.core.publisher.Flux;

public abstract class MessageProcessor {
	final Router<Message> upstream;
	final Router<Message> downstream;
	final Router<Message> abnormalstream;
	protected Publisher<Message> receiver;
	protected Subscriber<Message> sender;
	protected Subscriber<Message> abnormalSender;

	public MessageProcessor(Router<Message> upstream, Router<Message> downstream) {
		this(upstream, downstream, null);
	}

	public MessageProcessor(Router<Message> upstream, Router<Message> downstream, Router<Message> abnormalstream) {
		this.upstream = upstream;
		this.downstream = downstream;
		this.abnormalstream = abnormalstream;
	}

	protected abstract void onStart();

	protected abstract void onStop();

	public void start() {
		Thread t = new Thread(this::_start);
		t.start();
	}

	private void _start() {
		onStart();
		Publisher<Message> receiver = receiver();
		Subscriber<Message> sender = sender();
		Flux //
			.from(downstream) //
			.subscribe(sender) //
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
		Flux //
			.from(receiver) //
			.subscribe(upstream);
		;
	}

	public void stop() {
		upstream.stop();
		downstream.stop();
		if (abnormalstream != null) {
			abnormalstream.stop();
		}
		onStop();
	}

	protected void receiver(Publisher<Message> receiver) {
		this.receiver = receiver;
	}

	protected Publisher<Message> receiver() {
		return receiver;
	}

	protected void sender(Subscriber<Message> sender) {
		this.sender = sender;
	}

	protected Subscriber<Message> sender() {
		return sender;
	}

	protected Subscriber<Message> abnormalSender() {
		return abnormalSender;
	}
}
